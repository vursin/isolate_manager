import 'dart:async';
import 'dart:html';

import '../isolate_contactor.dart';
import '../utils/utils.dart';
import 'web_platform/isolate_contactor_web.dart';
import 'web_platform/isolate_contactor_web_worker.dart';

abstract class IsolateContactorInternal<T> implements IsolateContactor<T> {
  /// Create an instance
  static Future<IsolateContactorInternal<T>> create<T>({
    required FutureOr<T> Function(dynamic) function,
    required String workerName,
    required T Function(dynamic) converter,
    required T Function(dynamic) workerConverter,
    bool debugMode = true,
  }) async {
    /// If browser is not supported Worker then use Future
    if (workerName == '' || !Worker.supported) {
      if (workerName != '' && debugMode) {
        print(
            '[Isolate Manager]: This browser doesn\'t support Worker, Future will be applied!');
      }

      return IsolateContactorInternalFuture.create(
        function: function,
        functionName: workerName,
        converter: converter,
        workerConverter: workerConverter,
        debugMode: debugMode,
      );
    } else {
      return IsolateContactorInternalWorker.create(
        function: function,
        workerName: workerName,
        converter: converter,
        workerConverter: workerConverter,
        debugMode: debugMode,
      );
    }
  }

  /// Create modified isolate function
  static Future<IsolateContactorInternal<T>> createOwnIsolate<T>({
    required void Function(dynamic) isolateFunction,
    required String workerName,
    required dynamic initialParams,
    required T Function(dynamic) converter,
    required T Function(dynamic) workerConverter,
    bool debugMode = false,
  }) async {
    /// If browser is not supported Worker then use Future
    if (workerName == '' || !Worker.supported) {
      if (workerName != '' && debugMode) {
        print(
            '[Isolate Manager]: This browser doesn\'t support Worker, Future will be applied!');
      }

      return IsolateContactorInternalFuture.createOwnIsolate(
        isolateFunction: isolateFunction,
        isolateFunctionName: workerName,
        initialParams: initialParams,
        converter: converter,
        workerConverter: workerConverter,
        debugMode: debugMode,
      );
    } else {
      return IsolateContactorInternalWorker.createOwnIsolate(
        isolateFunction: isolateFunction,
        workerName: workerName,
        initialParams: initialParams,
        converter: converter,
        workerConverter: workerConverter,
        debugMode: debugMode,
      );
    }
  }

  @override
  Future<void> close() {
    throw UnimplementedError();
  }

  @override
  Future<void> dispose() {
    throw UnimplementedError();
  }

  @override
  bool get isComputing => throw UnimplementedError();

  @override
  Stream<ComputeState> get onComputeState => throw UnimplementedError();

  @override
  Stream<T> get onMessage => throw UnimplementedError();

  @override
  Future<void> restart() {
    throw UnimplementedError();
  }

  @override
  Future<T> sendMessage(message) {
    throw UnimplementedError();
  }

  @override
  Future<void> terminate() {
    throw UnimplementedError();
  }
}
