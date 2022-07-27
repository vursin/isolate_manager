import 'dart:async';

import '../../../isolate_contactor.dart';
import '../../isolate_contactor_controller/isolate_contactor_controller_web.dart';
import '../../utils/utils.dart';
import '../isolate_contactor_web.dart';

class IsolateContactorInternalFuture<T> implements IsolateContactorInternal<T> {
  /// For debugging
  bool _debugMode = false;

  /// Check for current isolate in bool
  bool _isComputing = false;

  /// Check for current cumputing state in enum with listener
  final StreamController<ComputeState> _computeStateStreamController =
      StreamController.broadcast();

  /// Check for current cumputing state in enum with listener
  final StreamController<T> _mainStreamController =
      StreamController.broadcast();

  /// Listener for result
  IsolateContactorController<T>? _isolateContactorController;

  /// Control the function of isolate
  late void Function(dynamic) _isolateFunction;

  /// Control the parameters of isolate
  late dynamic _isolateParam;

  // ignore: unused_field
  late String _workerName;

  late T Function(dynamic) _converter;
  late T Function(dynamic) _workerConverter;

  /// Create an instance
  IsolateContactorInternalFuture._({
    required FutureOr<void> Function(dynamic) isolateFunction,
    required String workerName,
    required dynamic isolateParam,
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

  /// Create an instance
  static Future<IsolateContactorInternalFuture<T>> create<T>({
    required FutureOr<T> Function(dynamic) function,
    required String functionName,
    required T Function(dynamic) converter,
    required T Function(dynamic) workerConverter,
    bool debugMode = true,
  }) async {
    IsolateContactorInternalFuture<T> isolateContactor =
        IsolateContactorInternalFuture._(
      isolateFunction: internalIsolateFunction,
      workerName: functionName,
      isolateParam: function,
      converter: converter,
      workerConverter: workerConverter,
      debugMode: debugMode,
    );

    await isolateContactor._initial();

    return isolateContactor;
  }

  /// Create modified isolate function
  static Future<IsolateContactorInternalFuture<T>> createOwnIsolate<T>({
    required void Function(dynamic) isolateFunction,
    required String isolateFunctionName,
    required dynamic initialParams,
    required T Function(dynamic) converter,
    required T Function(dynamic) workerConverter,
    bool debugMode = false,
  }) async {
    IsolateContactorInternalFuture<T> isolateContactor =
        IsolateContactorInternalFuture._(
      isolateFunction: isolateFunction,
      workerName: isolateFunctionName,
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
    _isolateContactorController = IsolateContactorControllerImpl(
      StreamController.broadcast(),
      converter: _converter,
      workerConverter: _workerConverter,
    );
    _isolateContactorController!.onMessage.listen((message) {
      _printDebug('[Main Stream] rawMessage = $message');
      _computeStateStreamController.sink.add(ComputeState.computed);
      _mainStreamController.sink.add(message);
      _isComputing = false;
    });

    _isolateFunction([_isolateParam, _isolateContactorController]);

    _isComputing = false;
    _computeStateStreamController.sink.add(ComputeState.computed);
    _printDebug('Initialized');
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
  ///
  /// Umplemented in web platform at the moment.
  @override
  Future<void> restart() async {
    if (_isolateContactorController == null) {
      _printDebug('! This isolate has been terminated');
      return;
    }

    _isolateContactorController!.close();

    _initial();
  }

  @override
  Future<void> close() => dispose();

  @override
  Future<void> terminate() => dispose();

  /// Dispose current [Isolate]
  @override
  Future<void> dispose() async {
    _isComputing = false;
    _isolateContactorController?.sendIsolate(IsolateState.dispose);
    _computeStateStreamController.sink.add(ComputeState.computed);

    _computeStateStreamController.close;
    await _isolateContactorController?.close();
    await _mainStreamController.close();

    _isolateContactorController = null;

    _printDebug('Disposed');
  }

  /// Send message to child isolate [function].
  ///
  /// Throw IsolateContactorException if error occurs.
  @override
  Future<T> sendMessage(dynamic message) {
    if (_isolateContactorController == null) {
      _printDebug('! This isolate has been terminated');
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
    _isolateContactorController!.onMessage.listen((result) {
      if (!completer.isCompleted) completer.complete(result);
    });

    _printDebug('Message send to isolate: $message');

    _isolateContactorController!.sendIsolate(message);

    return completer.future;
  }

  /// Print if [debugMode] is true
  void _printDebug(Object? object, [bool force = false]) {
    // ignore: avoid_print
    if (_debugMode && !force) print('[Isolate Contactor]: $object');
  }
}
