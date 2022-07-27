import 'dart:async';
import 'dart:isolate';

import '../isolate_contactor.dart';
import '../isolate_contactor_controller/isolate_contactor_controller_stub.dart';
import '../utils/utils.dart';

class IsolateContactorInternal<T> implements IsolateContactor<T> {
  /// For debugging
  bool _debugMode = false;

  /// Create receive port
  late ReceivePort _receivePort;

  /// Create isolate channel
  late IsolateContactorControllerImpl _isolateContactorController;

  /// Create isolate
  Isolate? _isolate;

  /// Check for current computing state in bool
  bool _isComputing = false;

  /// Check for current computing state in enum with listener
  final StreamController<ComputeState> _computeStateStreamController =
      StreamController.broadcast();

  /// Check for current computing state in enum with listener
  final StreamController<T> _mainStreamController =
      StreamController.broadcast();

  /// Control the function of isolate
  late void Function(dynamic) _isolateFunction;

  /// Control the parameters of isolate
  late dynamic _isolateParam;

  /// Only for web platform
  // ignore: unused_field
  late String _workerName;

  T Function(dynamic)? _converter;
  T Function(dynamic)? _workerConverter;

  /// Internal instance
  IsolateContactorInternal._({
    required FutureOr<void> Function(dynamic) isolateFunction,
    required dynamic isolateParam,
    required String workerName,
    required T Function(dynamic) converter,
    required T Function(dynamic) workerConverter,
    bool debugMode = false,
  }) {
    _debugMode = debugMode;
    _isolateFunction = isolateFunction;
    _workerName = workerName;
    _converter = converter;
    _workerConverter = workerConverter;
    _isolateParam = isolateParam;
  }

  /// Create an instance with build-in function
  static Future<IsolateContactorInternal<T>> create<T>({
    required FutureOr<T> Function(dynamic) function,
    required String workerName,
    required T Function(dynamic) converter,
    required T Function(dynamic) workerConverter,
    bool debugMode = true,
  }) async {
    IsolateContactorInternal<T> isolateContactor = IsolateContactorInternal._(
      isolateFunction: internalIsolateFunction,
      workerName: workerName,
      isolateParam: function,
      converter: converter,
      workerConverter: workerConverter,
      debugMode: debugMode,
    );

    await isolateContactor._initial();

    return isolateContactor;
  }

  /// Create an instance with your own function
  static Future<IsolateContactorInternal<T>> createOwnIsolate<T>({
    required void Function(dynamic) isolateFunction,
    required dynamic initialParams,
    required String workerName,
    required T Function(dynamic) converter,
    required T Function(dynamic) workerConverter,
    bool debugMode = false,
  }) async {
    IsolateContactorInternal<T> isolateContactor = IsolateContactorInternal._(
      isolateFunction: isolateFunction,
      workerName: workerName,
      isolateParam: initialParams ?? [],
      converter: converter,
      workerConverter: workerConverter,
      debugMode: debugMode,
    );

    await isolateContactor._initial();

    return isolateContactor;
  }

  /// Initialize
  Future<void> _initial() async {
    _receivePort = ReceivePort();
    _isolateContactorController = IsolateContactorControllerImpl(
      _receivePort,
      converter: _converter,
      workerConverter: _workerConverter,
    );
    _isolateContactorController.onMessage.listen((message) {
      _printDebug('Message received from isolate: $message');
      _computeStateStreamController.sink.add(ComputeState.computed);
      _mainStreamController.sink.add(message);
      _isComputing = false;
    });

    _isolate = await Isolate.spawn(
        _isolateFunction, [_isolateParam, _receivePort.sendPort]);

    _isComputing = false;
    _computeStateStreamController.sink.add(ComputeState.computed);

    _printDebug('Initialized');
  }

  Future<void> _dispose() async {
    _isolateContactorController.sendIsolate(IsolateState.dispose);
    await _isolateContactorController.close();
    _receivePort.close();
    _isolate!.kill(priority: Isolate.beforeNextEvent);
    _isolate = null;
    _isComputing = false;
    _computeStateStreamController.sink.add(ComputeState.computed);
  }

  /// Get current message as stream
  @override
  Stream<T> get onMessage => _mainStreamController.stream;

  /// Get current state
  @override
  Stream<ComputeState> get onComputeState =>
      _computeStateStreamController.stream;

  /// Is current isolate computing
  @override
  bool get isComputing => _isComputing;

  /// Restart current [Isolate]
  @override
  Future<void> restart() async {
    if (_isolate == null) {
      _printDebug('! This isolate was terminated');
      return;
    }
    await _dispose();
    await _initial();
    _printDebug('Restarted');
  }

  @override
  Future<void> close() => dispose();

  @override
  Future<void> terminate() => dispose();

  /// Dispose current [Isolate]
  @override
  Future<void> dispose() async {
    if (_isolate == null) {
      _printDebug('! This isolate was terminated');
      return;
    }

    await _dispose();
    await _computeStateStreamController.close();
    await _mainStreamController.close();
    _printDebug('Disposed');
  }

  /// Send message to child isolate [function]
  ///
  /// Throw IsolateContactorException if error occurs.
  @override
  Future<T> sendMessage(dynamic message) async {
    if (_isolate == null) {
      _printDebug('! This isolate was terminated');
      return throw IsolateContactorException('This isolate was terminated');
    }

    if (_isComputing) {
      _printDebug(
          '! This isolate is still being computed, so the current request has been revoked!');

      return throw IsolateContactorException(
          'This isolate is still being computed, so the current request has been revoked');
    }

    _isComputing = true;
    _computeStateStreamController.sink.add(ComputeState.computing);

    final Completer<T> completer = Completer();
    _isolateContactorController.onMessage.listen((result) {
      if (!completer.isCompleted) completer.complete(result);
    });

    _printDebug('Message send to isolate: $message');

    _isolateContactorController.sendIsolate(message);

    return completer.future;
  }

  /// Print if [debugMode] is true
  void _printDebug(Object? object) {
    // ignore: avoid_print
    if (_debugMode) print('[Isolate Contactor]: $object');
  }
}
