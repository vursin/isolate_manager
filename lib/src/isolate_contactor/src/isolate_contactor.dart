import 'dart:async';

import 'isolate_contactor/isolate_contactor_stub.dart'
    if (dart.library.html) 'isolate_contactor/isolate_contactor_web.dart';
import 'utils/utils.dart';

abstract class IsolateContactor<T> {
  /// The easy way to create isolate function
  ///
  /// `function` must be static or top-level function.
  ///
  /// `workerName` name of the function, also name of thing like `functionName.dart.js` on Web platform.
  /// If this value is not specified, the plugin will use `Future` instead of `Worker`.
  ///
  /// `converter` (for Native) convert result before sending to to the result.
  ///
  /// `workerConverter` (for Worker on Web) convert result before sending to to the result.
  ///
  /// `debugMode` allow printing debug data in console. Default is set to `false`.
  static Future<IsolateContactor<T>> create<T>(
    FutureOr<T> Function(dynamic) function, {
    String workerName = '',
    T Function(dynamic)? converter,
    T Function(dynamic)? workerConverter,
    bool debugMode = false,
  }) async {
    return await IsolateContactorInternal.create<T>(
      function: function,
      workerName: workerName,
      converter: converter ?? (result) => result,
      workerConverter: workerConverter ?? (result) => result,
      debugMode: debugMode,
    );
  }

  /// Create an instance with your own isolate function
  ///
  /// `isolateFunction` You can take a look at the example to see what you need to do
  /// to make it works.
  ///
  /// `functionName` name of the function, also name of thing like `functionName.dart.js` on Web platform.
  /// If this value is not specified, the plugin will use `Future` instead of `Worker`.
  ///
  /// `converter` (for Native) convert result before sending to to the result.
  ///
  /// `workerConverter` (for Worker on Web) convert result before sending to to the result.
  ///
  /// `isolateParams` is the list of parameters that you want to add to your [isolateFunction]
  /// `debugMode` allow printing debug data in console. Default is set to false.
  static Future<IsolateContactor<T>> createOwnIsolate<T>(
    FutureOr<void> Function(dynamic) isolateFunction, {
    String workerName = '',
    T Function(dynamic)? converter,
    T Function(dynamic)? workerConverter,
    dynamic initialParams,
    bool debugMode = false,
  }) async {
    return IsolateContactorInternal.createOwnIsolate<T>(
        isolateFunction: isolateFunction,
        workerName: workerName,
        converter: converter ?? (result) => result,
        workerConverter: workerConverter ?? (result) => result,
        initialParams: initialParams,
        debugMode: debugMode);
  }

  /// Send message to the `function` for computing
  ///
  /// Throw `IsolateContactorException` when error occurs.
  Future<T> sendMessage(dynamic message) => throw UnimplementedError();

  /// Listen to the result of the isolate
  Stream<T> get onMessage => throw UnimplementedError();

  /// Listen to the current state of isolate.
  /// Includes `ComputeState.computed` and `ComputeState.computing`
  Stream<ComputeState> get onComputeState => throw UnimplementedError();

  /// Get current computing state of the isolate
  bool get isComputing => throw UnimplementedError();

  /// Restart the paused isolate
  ///
  /// This method is not available on Web platform at the moment
  Future<void> restart() async => throw UnimplementedError();

  /// Close current isolate, the same behavior with `dispose`
  Future<void> close() => throw UnimplementedError();

  /// Close current isolate, the same behavior with `dispose`
  Future<void> terminate() => throw UnimplementedError();

  /// Dispose current isolate
  Future<void> dispose() => throw UnimplementedError();
}
