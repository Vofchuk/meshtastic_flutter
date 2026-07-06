import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:flutter/foundation.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'package:logging/logging.dart';
import 'package:permission_handler/permission_handler.dart';

import '../generated/admin.pb.dart';
import '../generated/mesh.pb.dart';
import '../generated/config.pb.dart';
import '../generated/module_config.pb.dart';
import '../generated/channel.pb.dart';
import '../generated/portnums.pb.dart';
import 'channel_utils.dart';
import 'models/connection_state.dart';
import 'models/mesh_packet_wrapper.dart';
import 'models/node_info.dart';
import 'models/meshtastic_config.dart';
import 'exceptions/meshtastic_exceptions.dart';

/// Main client for communicating with Meshtastic devices over BLE
class MeshtasticClient {
  static final Logger _logger = Logger('MeshtasticClient');

  // Meshtastic BLE Service UUID
  static const String _serviceUuid = '6ba1b218-15a8-461f-9fa8-5dcae273eafd';

  // Characteristic UUIDs
  static const String _toRadioUuid = 'f75c76d2-129e-4dad-a1dd-7866124401e7';
  static const String _fromRadioUuid = '2c55e69e-4993-11ed-b878-0242ac120002';
  static const String _fromNumUuid = 'ed9da18c-a800-4f66-a670-aa7547e34453';

  // Maximum packet size
  static const int _maxPacketSize = 512;

  /// Broadcast destination node ID (all nodes).
  static const int broadcastId = 0xFFFFFFFF;

  // Private fields
  BluetoothDevice? _device;
  BluetoothCharacteristic? _toRadioChar;
  BluetoothCharacteristic? _fromRadioChar;
  BluetoothCharacteristic? _fromNumChar;

  StreamSubscription<BluetoothConnectionState>? _connectionSubscription;
  StreamSubscription<List<int>>? _fromNumSubscription;

  final StreamController<ConnectionStatus> _connectionController =
      StreamController<ConnectionStatus>.broadcast();
  final StreamController<MeshPacketWrapper> _packetController =
      StreamController<MeshPacketWrapper>.broadcast();
  final StreamController<NodeInfoWrapper> _nodeController =
      StreamController<NodeInfoWrapper>.broadcast();

  // Configuration and state
  final Map<int, NodeInfoWrapper> _nodes = {};
  MyNodeInfo? _myNodeInfo;
  Config? _config;
  ModuleConfig? _moduleConfig;
  final List<Channel> _channels = [];
  User? _localUser;

  bool _configComplete = false;
  int _expectedFromNum = 0;

  Uint8List? _sessionPasskey;
  int _adminPacketId = 0;
  final Map<int, Completer<AdminMessage>> _pendingAdminResponses = {};

  // Public streams
  Stream<ConnectionStatus> get connectionStream => _connectionController.stream;
  Stream<MeshPacketWrapper> get packetStream => _packetController.stream;
  Stream<NodeInfoWrapper> get nodeStream => _nodeController.stream;

  // Getters for current state
  Map<int, NodeInfoWrapper> get nodes => Map.unmodifiable(_nodes);
  MyNodeInfo? get myNodeInfo => _myNodeInfo;
  MeshtasticConfigWrapper? get config =>
      _config != null && _moduleConfig != null
      ? MeshtasticConfigWrapper(
          config: _config!,
          moduleConfig: _moduleConfig!,
          channels: _channels,
        )
      : null;
  User? get localUser => _localUser;
  bool get isConnected => _device?.isConnected ?? false;
  bool get isConfigured => _configComplete;

  int get myNodeNum => _myNodeInfo?.myNodeNum ?? 0;

  List<Channel> get channels => List.unmodifiable(_channels);

  /// Parses a Meshtastic PSK string (Base64 URL-safe/standard or hex).
  static Uint8List parsePsk(String input) => ChannelUtils.parsePsk(input);

  /// Finds a configured channel index matching [name], or null.
  int? findChannelIndexByName({required String name}) {
    return ChannelUtils.findChannelIndexByName(_channels, name: name);
  }

  /// Finds a configured channel index matching [name] and [psk], or null.
  int? findChannelIndex({
    required String name,
    required Uint8List psk,
  }) {
    return ChannelUtils.findChannelIndex(
      _channels,
      name: name,
      psk: psk,
    );
  }

  /// Initialize the client and request necessary permissions
  Future<void> initialize() async {
    _logger.info('Initializing Meshtastic client');

    // Check if Bluetooth is supported
    if (await FlutterBluePlus.isSupported == false) {
      throw const BluetoothException('Bluetooth not supported on this device');
    }

    // Request permissions
    await _requestPermissions();

    // Check if Bluetooth is enabled
    final state = await FlutterBluePlus.adapterState.first;
    if (state != BluetoothAdapterState.on) {
      throw const BluetoothException('Bluetooth is not enabled');
    }

    _logger.info('Meshtastic client initialized successfully');
  }

  /// Request necessary permissions for BLE
  Future<void> _requestPermissions() async {
    final permissions = [
      Permission.bluetooth,
      Permission.bluetoothConnect,
      Permission.bluetoothScan,
      Permission.locationWhenInUse,
    ];

    for (final permission in permissions) {
      final status = await permission.request();
      if (!status.isGranted) {
        throw PermissionException('Permission denied: $permission');
      }
    }
  }

  /// Scan for nearby Meshtastic devices
  Stream<BluetoothDevice> scanForDevices({
    Duration timeout = const Duration(seconds: 15),
  }) {
    late StreamController<BluetoothDevice> controller;
    controller = StreamController<BluetoothDevice>(
      onListen: () => unawaited(_runDeviceScan(controller, timeout)),
    );
    return controller.stream;
  }

  Future<void> _runDeviceScan(
    StreamController<BluetoothDevice> controller,
    Duration timeout,
  ) async {
    final seen = <String>{};
    StreamSubscription<List<ScanResult>>? scanSub;

    try {
      for (final device in await FlutterBluePlus.bondedDevices) {
        if (!_isMeshtasticDevice(device)) continue;
        if (seen.add(device.remoteId.str)) {
          _logger.info(
            'Bonded Meshtastic device: ${device.platformName} (${device.remoteId})',
          );
          controller.add(device);
        }
      }

      for (final device in await _loadSystemMeshtasticDevices()) {
        if (seen.add(device.remoteId.str)) {
          _logger.info(
            'Known Meshtastic device: ${device.platformName} (${device.remoteId})',
          );
          controller.add(device);
        }
      }

      await FlutterBluePlus.stopScan();

      scanSub = FlutterBluePlus.onScanResults.listen((results) {
        for (final result in results) {
          if (!_isMeshtasticScanResult(result)) continue;
          final device = result.device;
          if (seen.add(device.remoteId.str)) {
            _logger.info(
              'Found Meshtastic device: ${device.platformName} (${device.remoteId}) '
              'adv="${result.advertisementData.advName}"',
            );
            controller.add(device);
          }
        }
      });

      // Do not use withNames — flutter_blue_plus requires an exact name match,
      // so "Meshtastic_c6c8" would be missed. Filter in Dart instead.
      await FlutterBluePlus.startScan(
        timeout: timeout,
        androidUsesFineLocation: true,
        androidScanMode: AndroidScanMode.lowLatency,
      );

      await Future<void>.delayed(timeout);
    } catch (error, stackTrace) {
      _logger.warning('Scan failed: $error');
      if (!controller.isClosed) {
        controller.addError(error, stackTrace);
      }
    } finally {
      await scanSub?.cancel();
      await FlutterBluePlus.stopScan();
      if (!controller.isClosed) {
        await controller.close();
      }
    }
  }

  Future<List<BluetoothDevice>> _loadSystemMeshtasticDevices() async {
    try {
      return await FlutterBluePlus.systemDevices([Guid(_serviceUuid)]);
    } catch (error) {
      _logger.fine('systemDevices unavailable: $error');
      return const [];
    }
  }

  bool _isMeshtasticDevice(BluetoothDevice device) {
    return device.platformName.toLowerCase().contains('meshtastic');
  }

  bool _isMeshtasticScanResult(ScanResult result) {
    if (_isMeshtasticDevice(result.device)) return true;

    final advName = result.advertisementData.advName;
    if (advName.toLowerCase().contains('meshtastic')) return true;

    return result.advertisementData.serviceUuids.any(
      (uuid) => uuid.toString().toLowerCase() == _serviceUuid.toLowerCase(),
    );
  }

  /// Connect to a device by BLE address (e.g. 10:51:DB:2A:C6:C9).
  Future<void> connectToAddress(String address) async {
    final normalized = _normalizeMacAddress(address);
    final device = BluetoothDevice.fromId(normalized);
    await connectToDevice(device);
  }

  String _normalizeMacAddress(String address) {
    final trimmed = address.trim().toUpperCase();
    if (trimmed.contains(':')) return trimmed;
    if (trimmed.length != 12) {
      throw ArgumentError.value(address, 'address', 'Expected MAC like 10:51:DB:2A:C6:C9');
    }
    final pairs = <String>[];
    for (var i = 0; i < 12; i += 2) {
      pairs.add(trimmed.substring(i, i + 2));
    }
    return pairs.join(':');
  }

  /// Connect to a specific Meshtastic device
  Future<void> connectToDevice(BluetoothDevice device) async {
    _logger.info(
      'Connecting to device: ${device.platformName} (${device.remoteId})',
    );

    try {
      _emitConnectionState(MeshtasticConnectionState.connecting);

      // Disconnect from any existing device
      await disconnect();

      _device = device;

      // Listen for connection state changes
      _connectionSubscription = device.connectionState.listen((state) {
        if (state == BluetoothConnectionState.disconnected) {
          _handleDisconnection();
        }
      });

      // Connect to device
      await device.connect(timeout: const Duration(seconds: 30));

      // Discover services
      final services = await device.discoverServices();
      final meshtasticService = services.firstWhere(
        (service) =>
            service.uuid.toString().toLowerCase() == _serviceUuid.toLowerCase(),
        orElse: () =>
            throw const ConnectionException('Meshtastic service not found'),
      );

      // Get characteristics
      _toRadioChar = meshtasticService.characteristics.firstWhere(
        (char) =>
            char.uuid.toString().toLowerCase() == _toRadioUuid.toLowerCase(),
        orElse: () =>
            throw const ConnectionException('ToRadio characteristic not found'),
      );

      _fromRadioChar = meshtasticService.characteristics.firstWhere(
        (char) =>
            char.uuid.toString().toLowerCase() == _fromRadioUuid.toLowerCase(),
        orElse: () => throw const ConnectionException(
          'FromRadio characteristic not found',
        ),
      );

      _fromNumChar = meshtasticService.characteristics.firstWhere(
        (char) =>
            char.uuid.toString().toLowerCase() == _fromNumUuid.toLowerCase(),
        orElse: () =>
            throw const ConnectionException('FromNum characteristic not found'),
      );

      // Log characteristic properties for debugging
      _logger.info(
        'ToRadio properties: write=${_toRadioChar!.properties.write}, '
        'writeWithoutResponse=${_toRadioChar!.properties.writeWithoutResponse}',
      );
      _logger.info(
        'FromRadio properties: read=${_fromRadioChar!.properties.read}, '
        'notify=${_fromRadioChar!.properties.notify}',
      );
      _logger.info(
        'FromNum properties: read=${_fromNumChar!.properties.read}, '
        'notify=${_fromNumChar!.properties.notify}',
      );

      // Set MTU to 512
      await device.requestMtu(512);

      // Enable notifications on FromNum
      await _fromNumChar!.setNotifyValue(true);
      _fromNumSubscription = _fromNumChar!.lastValueStream.listen(
        _handleFromNumNotification,
      );

      _emitConnectionState(MeshtasticConnectionState.configuring);

      // Start configuration process
      await _startConfiguration();

      _logger.info('Successfully connected to device');
    } catch (e) {
      _logger.severe('Failed to connect to device: $e');
      _emitConnectionState(
        MeshtasticConnectionState.error,
        errorMessage: e.toString(),
      );
      rethrow;
    }
  }

  /// Disconnect from the current device
  Future<void> disconnect({bool emitState = true}) async {
    _logger.info('Disconnecting from device');

    await _fromNumSubscription?.cancel();
    _fromNumSubscription = null;

    await _connectionSubscription?.cancel();
    _connectionSubscription = null;

    if (_device?.isConnected == true) {
      await _device!.disconnect();
    }

    _device = null;
    _toRadioChar = null;
    _fromRadioChar = null;
    _fromNumChar = null;

    _configComplete = false;
    _expectedFromNum = 0;
    _nodes.clear();
    _myNodeInfo = null;
    _config = null;
    _moduleConfig = null;
    _channels.clear();
    _localUser = null;
    resetAdminSession();

    if (emitState) {
      _emitConnectionState(MeshtasticConnectionState.disconnected);
    }
  }

  /// Send a text message to a specific node or broadcast
  Future<void> sendTextMessage(
    String message, {
    int? destinationId,
    int channel = 0,
  }) async {
    if (!isConnected) {
      throw const ConnectionException('Not connected to a device');
    }

    if (!isConfigured) {
      throw const ConnectionException('Device configuration not complete');
    }

    // Generate a random packet ID
    final packetId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;

    final packet = MeshPacket(
      from: _myNodeInfo?.myNodeNum ?? 0, // Set sender node ID
      to: destinationId ?? 0xFFFFFFFF, // 0xFFFFFFFF for broadcast
      channel: channel,
      id: packetId,
      decoded: Data(
        portnum: PortNum.TEXT_MESSAGE_APP,
        payload: utf8.encode(message),
      ),
      wantAck: destinationId != null, // Request ACK for direct messages
      hopLimit: 3,
      priority: MeshPacket_Priority.DEFAULT,
    );

    _logger.info(
      'Sending text message: "$message" from ${packet.from.toRadixString(16)} to ${packet.to.toRadixString(16)} on channel $channel',
    );
    await sendMeshPacket(packet);
  }

  /// Send a position update
  Future<void> sendPosition(
    double latitude,
    double longitude, {
    int? altitude,
  }) async {
    if (!isConnected) {
      throw const ConnectionException('Not connected to a device');
    }

    if (!isConfigured) {
      throw const ConnectionException('Device configuration not complete');
    }

    final position = Position(
      latitudeI: (latitude * 1e7).round(),
      longitudeI: (longitude * 1e7).round(),
      altitude: altitude,
      time: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );

    // Generate a random packet ID
    final packetId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;

    final packet = MeshPacket(
      from: _myNodeInfo?.myNodeNum ?? 0, // Set sender node ID
      to: 0xFFFFFFFF, // Broadcast
      id: packetId,
      decoded: Data(
        portnum: PortNum.POSITION_APP,
        payload: position.writeToBuffer(),
      ),
      hopLimit: 3,
      priority: MeshPacket_Priority.DEFAULT,
    );

    _logger.info(
      'Sending position: lat=$latitude, lon=$longitude, alt=$altitude',
    );
    await sendMeshPacket(packet);
  }

  /// Send an arbitrary binary payload on a custom port number.
  ///
  /// [portNum] must be a valid Meshtastic port (see `portnums.proto` in the
  /// fork). Private application ports are in the 256–511 range; port 256 is
  /// `PortNum.PRIVATE_APP`.
  Future<void> sendData({
    required int portNum,
    required Uint8List payload,
    int destinationId = broadcastId,
    int channel = 0,
    bool wantAck = false,
  }) async {
    if (!isConnected) {
      throw const ConnectionException('Not connected to a device');
    }

    if (!isConfigured) {
      throw const ConnectionException('Device configuration not complete');
    }

    final resolvedPort = PortNum.valueOf(portNum);
    if (resolvedPort == null) {
      throw ArgumentError.value(
        portNum,
        'portNum',
        'Unknown port number; use a value defined in portnums.proto',
      );
    }

    final packetId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;

    final packet = MeshPacket(
      from: _myNodeInfo?.myNodeNum ?? 0,
      to: destinationId,
      channel: channel,
      id: packetId,
      decoded: Data(
        portnum: resolvedPort,
        payload: payload,
      ),
      wantAck: wantAck,
      hopLimit: 3,
      priority: MeshPacket_Priority.DEFAULT,
    );

    _logger.info(
      'Sending data: portNum=$portNum, ${payload.length} bytes '
      'from ${packet.from.toRadixString(16)} to ${packet.to.toRadixString(16)} '
      'on channel $channel',
    );
    await sendMeshPacket(packet);
  }

  void _assertConnectedAndConfigured() {
    if (!isConnected) {
      throw const ConnectionException('Not connected to a device');
    }
    if (!isConfigured) {
      throw const ConnectionException('Device configuration not complete');
    }
    if (myNodeNum == 0) {
      throw const ConfigurationException('Local node ID not available');
    }
  }

  void _updateChannelInCache(Channel channel) {
    if (channel.index < _channels.length) {
      _channels[channel.index] = channel;
    } else {
      while (_channels.length <= channel.index) {
        _channels.add(Channel());
      }
      _channels[channel.index] = channel;
    }
  }

  /// Send a packet to the device
  Future<void> sendMeshPacket(MeshPacket packet) async {
    if (_toRadioChar == null) {
      throw const ConnectionException('ToRadio characteristic not available');
    }

    final toRadio = ToRadio(packet: packet);
    final data = toRadio.writeToBuffer();

    if (data.length > _maxPacketSize) {
      throw const ProtocolException('Packet too large');
    }

    _logger.info(
      'Sending packet: from=${packet.from.toRadixString(16)}, to=${packet.to.toRadixString(16)}, '
      'id=${packet.id}, portnum=${packet.decoded.portnum}, size=${data.length} bytes',
    );

    // Check if characteristic supports write without response
    final supportsWriteWithoutResponse =
        _toRadioChar!.properties.writeWithoutResponse;

    // Android/iOS cap single withResponse writes at 252 B unless longWrite is used.
    final allowLongWrite =
        !supportsWriteWithoutResponse && data.length > 252;

    await _toRadioChar!.write(
      data,
      withoutResponse: supportsWriteWithoutResponse,
      allowLongWrite: allowLongWrite,
    );

    _logger.fine('Packet sent successfully');
  }

  /// Start the configuration process
  Future<void> _startConfiguration() async {
    _logger.info('Starting configuration process');

    // Send wantConfigId to start configuration download
    final wantConfig = ToRadio(wantConfigId: 0);
    // Check if characteristic supports write without response
    final supportsWriteWithoutResponse =
        _toRadioChar!.properties.writeWithoutResponse;
    await _toRadioChar!.write(
      wantConfig.writeToBuffer(),
      withoutResponse: supportsWriteWithoutResponse,
    );

    // Start reading configuration data
    await _readConfiguration();
  }

  /// Read configuration data from the device
  Future<void> _readConfiguration() async {
    _logger.info('Reading configuration from device');

    while (!_configComplete) {
      try {
        final data = await _fromRadioChar!.read();
        if (data.isEmpty) {
          _logger.info('Configuration complete - received empty packet');
          _configComplete = true;
          _emitConnectionState(MeshtasticConnectionState.connected);
          break;
        }

        await _processFromRadioData(data);

        // Small delay to prevent overwhelming the device
        await Future.delayed(const Duration(milliseconds: 50));
      } catch (e) {
        _logger.warning('Error reading configuration: $e');
        break;
      }
    }
  }

  /// Process incoming data from FromRadio characteristic
  Future<void> _processFromRadioData(List<int> data) async {
    try {
      final fromRadio = FromRadio.fromBuffer(data);
      _logger.fine('Received FromRadio: ${fromRadio.toString()}');

      if (fromRadio.hasMyInfo()) {
        _myNodeInfo = fromRadio.myInfo;
        _logger.info(
          'Received MyNodeInfo: myNodeNum=${_myNodeInfo!.myNodeNum.toRadixString(16)}',
        );
      }

      if (fromRadio.hasNodeInfo()) {
        final nodeInfo = NodeInfoWrapper(fromRadio.nodeInfo);
        _nodes[nodeInfo.num] = nodeInfo;
        _nodeController.add(nodeInfo);
        _logger.info(
          'Received NodeInfo: num=${nodeInfo.num.toRadixString(16)}, '
          'displayName=${nodeInfo.displayName}',
        );

        // Extract user info from the node info
        if (nodeInfo.user != null &&
            _localUser == null &&
            _myNodeInfo != null &&
            nodeInfo.num == _myNodeInfo!.myNodeNum) {
          _localUser = nodeInfo.user;
          _logger.info(
            'Received local User: longName=${_localUser!.longName}, '
            'shortName=${_localUser!.shortName}',
          );
        }
      }

      if (fromRadio.hasConfig()) {
        _config = fromRadio.config;
        _logger.info('Received Config');
      }

      if (fromRadio.hasModuleConfig()) {
        _moduleConfig = fromRadio.moduleConfig;
        _logger.info('Received ModuleConfig');
      }

      if (fromRadio.hasChannel()) {
        final channel = fromRadio.channel;
        if (channel.index < _channels.length) {
          _channels[channel.index] = channel;
        } else {
          while (_channels.length <= channel.index) {
            _channels.add(Channel());
          }
          _channels[channel.index] = channel;
        }
        _logger.info('Received Channel ${channel.index}');
      }

      if (fromRadio.hasPacket()) {
        _handleAdminPacket(fromRadio.packet);
        final packetWrapper = MeshPacketWrapper(fromRadio.packet);
        _packetController.add(packetWrapper);
        _logger.info('Received MeshPacket: ${packetWrapper.toString()}');
      }

      if (fromRadio.hasConfigCompleteId()) {
        _logger.info('Configuration complete');
        _configComplete = true;
        _emitConnectionState(MeshtasticConnectionState.connected);

        // Log summary of received configuration
        _logger.info('Configuration summary:');
        _logger.info('  MyNodeInfo: ${_myNodeInfo != null ? "✓" : "✗"}');
        _logger.info('  Config: ${_config != null ? "✓" : "✗"}');
        _logger.info('  ModuleConfig: ${_moduleConfig != null ? "✓" : "✗"}');
        _logger.info('  Channels: ${_channels.length}');
        _logger.info('  Nodes: ${_nodes.length}');
        _logger.info('  LocalUser: ${_localUser != null ? "✓" : "✗"}');
      }
    } catch (e) {
      _logger.warning('Error processing FromRadio data: $e');
      throw ProtocolException('Failed to parse FromRadio data', e);
    }
  }

  /// Handle FromNum notifications
  void _handleFromNumNotification(List<int> data) {
    if (data.length >= 4) {
      final bytes = Uint8List.fromList(data);
      final fromNum = ByteData.view(bytes.buffer).getUint32(0, Endian.little);
      _logger.fine(
        'FromNum notification: $fromNum (expected: $_expectedFromNum)',
      );

      if (fromNum > _expectedFromNum) {
        _expectedFromNum = fromNum;
        // Read new data from FromRadio
        _readFromRadio();
      }
    }
  }

  /// Read available data from FromRadio
  Future<void> _readFromRadio() async {
    try {
      while (true) {
        final data = await _fromRadioChar!.read();
        if (data.isEmpty) break;

        await _processFromRadioData(data);
      }
    } catch (e) {
      _logger.warning('Error reading from FromRadio: $e');
    }
  }

  /// Handle disconnection
  void _handleDisconnection() {
    _logger.info('Device disconnected');
    _emitConnectionState(MeshtasticConnectionState.disconnected);
  }

  /// Emit connection state change
  void _emitConnectionState(
    MeshtasticConnectionState state, {
    String? errorMessage,
  }) {
    if (_connectionController.isClosed) return;

    final status = ConnectionStatus(
      state: state,
      deviceAddress: _device?.remoteId.toString(),
      deviceName: _device?.platformName,
      errorMessage: errorMessage,
      timestamp: DateTime.now(),
    );

    _connectionController.add(status);
  }

  /// Cached session passkey from the last admin handshake, if any.
  Uint8List? get sessionPasskey => _sessionPasskey;

  void resetAdminSession() {
    _sessionPasskey = null;
    for (final pending in _pendingAdminResponses.values) {
      if (!pending.isCompleted) {
        pending.completeError(
          const ConnectionException('Admin session reset'),
        );
      }
    }
    _pendingAdminResponses.clear();
  }

  void _handleAdminPacket(MeshPacket packet) {
    if (!packet.hasDecoded()) return;
    if (packet.decoded.portnum != PortNum.ADMIN_APP) return;

    AdminMessage response;
    try {
      response = AdminMessage.fromBuffer(packet.decoded.payload);
    } catch (error) {
      _logger.warning('Failed to parse AdminMessage: $error');
      return;
    }

    if (response.sessionPasskey.isNotEmpty) {
      _sessionPasskey = Uint8List.fromList(response.sessionPasskey);
      _logger.fine(
        'Updated session passkey (${_sessionPasskey!.length} bytes)',
      );
    }

    final requestId = packet.decoded.requestId;
    if (requestId != 0) {
      final pending = _pendingAdminResponses.remove(requestId);
      pending?.complete(response);
    } else if (_pendingAdminResponses.length == 1 &&
        response.hasGetOwnerResponse()) {
      final entry = _pendingAdminResponses.entries.first;
      entry.value.complete(response);
      _pendingAdminResponses.remove(entry.key);
    }
  }

  Future<AdminMessage> sendAdminMessage(
    AdminMessage message, {
    bool stateChanging = true,
    Duration timeout = const Duration(seconds: 10),
  }) async {
    _assertConnectedAndConfigured();

    if (stateChanging) {
      await ensureSessionPasskey(timeout: timeout);
      message.sessionPasskey = _sessionPasskey!;
    }

    final packetId = _nextAdminPacketId();
    final completer = Completer<AdminMessage>();
    _pendingAdminResponses[packetId] = completer;

    final packet = MeshPacket(
      from: myNodeNum,
      to: myNodeNum,
      channel: 0,
      id: packetId,
      decoded: Data(
        portnum: PortNum.ADMIN_APP,
        payload: message.writeToBuffer(),
        requestId: packetId,
      ),
      hopLimit: 3,
      priority: MeshPacket_Priority.RELIABLE,
    );

    try {
      await sendMeshPacket(packet);
      return await completer.future.timeout(
        timeout,
        onTimeout: () {
          _pendingAdminResponses.remove(packetId);
          throw TimeoutException(
            'Admin request timed out after ${timeout.inSeconds}s',
          );
        },
      );
    } catch (error) {
      _pendingAdminResponses.remove(packetId);
      rethrow;
    }
  }

  Future<void> _sendAdminMessageNoWait(
    AdminMessage message, {
    bool stateChanging = true,
  }) async {
    _assertConnectedAndConfigured();

    if (stateChanging) {
      await ensureSessionPasskey();
      message.sessionPasskey = _sessionPasskey!;
    }

    final packetId = _nextAdminPacketId();
    final packet = MeshPacket(
      from: myNodeNum,
      to: myNodeNum,
      channel: 0,
      id: packetId,
      decoded: Data(
        portnum: PortNum.ADMIN_APP,
        payload: message.writeToBuffer(),
        requestId: packetId,
      ),
      hopLimit: 3,
      priority: MeshPacket_Priority.RELIABLE,
    );

    await sendMeshPacket(packet);
    await Future<void>.delayed(const Duration(milliseconds: 150));
  }

  Future<void> ensureSessionPasskey({
    Duration timeout = const Duration(seconds: 10),
  }) async {
    if (_sessionPasskey != null && _sessionPasskey!.isNotEmpty) return;

    final response = await sendAdminMessage(
      AdminMessage(getOwnerRequest: true),
      stateChanging: false,
      timeout: timeout,
    );

    if (_sessionPasskey == null || _sessionPasskey!.isEmpty) {
      if (response.sessionPasskey.isNotEmpty) {
        _sessionPasskey = Uint8List.fromList(response.sessionPasskey);
      }
    }

    if (_sessionPasskey == null || _sessionPasskey!.isEmpty) {
      throw const ConfigurationException(
        'Device did not provide an admin session passkey',
      );
    }
  }

  Future<void> setChannel({
    required int index,
    required String name,
    required Uint8List psk,
    Channel_Role role = Channel_Role.SECONDARY,
  }) async {
    if (index < 0 || index >= ChannelUtils.maxChannelSlots) {
      throw ArgumentError.value(index, 'index', 'Channel index must be 0..7');
    }
    if (name.isEmpty) {
      throw ArgumentError.value(name, 'name', 'Channel name must not be empty');
    }
    if (psk.isEmpty) {
      throw ArgumentError.value(psk, 'psk', 'PSK must not be empty');
    }

    final channel = Channel(
      index: index,
      role: role,
      settings: ChannelSettings(
        name: name,
        psk: psk,
        uplinkEnabled: false,
        downlinkEnabled: false,
      ),
    );

    await _sendAdminMessageNoWait(AdminMessage(beginEditSettings: true));
    await _sendAdminMessageNoWait(AdminMessage(setChannel: channel));
    await _sendAdminMessageNoWait(AdminMessage(commitEditSettings: true));

    _updateChannelInCache(channel);
    _logger.info('Set channel $index "$name" (${psk.length}-byte PSK)');
  }

  /// Ensures [name]/[psk] exist on the node; returns the channel index.
  ///
  /// Never overwrites a channel that already exists with the same [name] —
  /// manual configuration via the official Meshtastic app is preserved.
  Future<int> ensureChannel({
    required String name,
    required Uint8List psk,
  }) async {
    _assertConnectedAndConfigured();

    final existingByName = ChannelUtils.findChannelIndexByName(
      channels,
      name: name,
    );
    if (existingByName != null) {
      _logger.info(
        'Channel "$name" already present at index $existingByName — leaving unchanged',
      );
      return existingByName;
    }

    final existing = ChannelUtils.findChannelIndex(
      channels,
      name: name,
      psk: psk,
    );
    if (existing != null) return existing;

    final targetIndex = ChannelUtils.findNextSecondarySlot(channels) ??
        (throw const ConfigurationException(
          'No free secondary channel slot (max 8 channels)',
        ));

    _logger.info('Creating channel $targetIndex "$name"');

    await setChannel(
      index: targetIndex,
      name: name,
      psk: psk,
    );

    return targetIndex;
  }

  int _nextAdminPacketId() {
    _adminPacketId = (_adminPacketId + 1) & 0xFFFFFFFF;
    if (_adminPacketId == 0) _adminPacketId = 1;
    return _adminPacketId;
  }

  /// Dispose of the client and clean up resources
  void dispose() {
    _logger.info('Disposing Meshtastic client');

    disconnect(emitState: false);
    _connectionController.close();
    _packetController.close();
    _nodeController.close();
  }
}
