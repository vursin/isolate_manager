import 'dart:async';

import 'package:stream_channel/isolate_channel.dart';

import '../isolate_contactor_controller.dart';
import '../utils/utils.dart';

class IsolateContactorControllerImpl<T>
    implements IsolateContactorController<T> {
  late IsolateChannel _delegate;
  late StreamSubscription _delegateSubscription;

  final StreamController<T> _mainStreamController =
      StreamController.broadcast();
  final StreamController _isolateStreamController =
      StreamController.broadcast();

  final Function()? onDispose;
  final T Function(dynamic)? converter;
  dynamic _initialParams;

  IsolateContactorControllerImpl(
    dynamic params, {
    this.onDispose,
    this.converter, // Converter for native
    T Function(dynamic)? workerConverter, // Converter for Worker (Web Only)
  }) {
    if (params is List) {
      _delegate = IsolateChannel.connectSend(params.last);
      _initialParams = params.first;
    } else {
      _delegate = IsolateChannel.connectReceive(params);
    }

    _delegateSubscription = _delegate.stream.listen((event) {
      (event as Map<IsolatePort, dynamic>).forEach((key, value) {
        switch (key) {
          case IsolatePort.main:
            _mainStreamController
                .add(converter == null ? value : converter!(value));
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

  /// Only need for web platform
  @override
  IsolateChannel get controller => _delegate;

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
      _delegateSubscription.cancel(),
      _mainStreamController.close(),
      _isolateStreamController.close(),
    ]);
  }
}
