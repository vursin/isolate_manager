import 'dart:async';

import '../../utils/utils.dart';
import '../isolate_contactor_controller_web.dart';

class IsolateContactorControllerImplFuture<T>
    implements IsolateContactorControllerImpl<T> {
  late StreamController _delegate;

  final StreamController<T> _mainStreamController =
      StreamController.broadcast();
  final StreamController _isolateStreamController =
      StreamController.broadcast();

  final Function()? onDispose;
  final T Function(dynamic) converter;
  dynamic _initialParams;

  IsolateContactorControllerImplFuture(
    dynamic params, {
    this.onDispose,
    required this.converter,
    required T Function(dynamic) workerConverter,
  }) {
    if (params is List) {
      _delegate = params.last.controller;
      _initialParams = params.first;
    } else {
      _delegate = params;
    }

    _delegate.stream.listen((event) {
      (event as Map<IsolatePort, dynamic>).forEach((key, value) {
        switch (key) {
          case IsolatePort.main:
            _mainStreamController.add(converter(value));
            break;
          case IsolatePort.isolate:
            if (value == IsolateState.dispose) {
              if (onDispose != null) onDispose!();
              close();
            } else {
              _isolateStreamController.add(value);
            }
            break;
        }
      });
    });
  }

  @override
  StreamController get controller => _delegate;

  /// Get initial params for `createOwnIsolate`
  @override
  dynamic get initialParams => _initialParams;

  @override
  Stream<T> get onMessage => _mainStreamController.stream;

  @override
  Stream get onIsolateMessage => _isolateStreamController.stream;

  @override
  void sendIsolate(dynamic message) {
    try {
      _delegate.sink.add({IsolatePort.isolate: message});
    } catch (_) {
      // The delegate may be closed
    }
  }

  @override
  void sendResult(T message) {
    try {
      _delegate.sink.add({IsolatePort.main: message});
    } catch (_) {
      // The delegate may be closed
    }
  }

  @override
  Future<void> close() async {
    await Future.wait([
      _delegate.close(),
      _mainStreamController.close(),
      _isolateStreamController.close(),
    ]);
  }
}
