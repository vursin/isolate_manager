import 'isolate_contactor/isolate_contactor.dart';

class IsolateManagerController<T> implements IsolateContactorController<T> {
  late IsolateContactorController<T> _delegate;

  /// This method only use to create your own isolate.
  ///
  /// `params` is the params of the function.
  /// `onDispose` will be called when the controller is disposed.
  IsolateManagerController(
    dynamic params, {
    Function()? onDispose,
  }) {
    _delegate = IsolateContactorController(
      params,
      onDispose: onDispose,
    );
  }

  @override
  Future<void> close() => _delegate.close();

  @override
  get controller => _delegate.controller;

  @override
  get initialParams => _delegate.initialParams;

  @override
  Stream get onIsolateMessage => _delegate.onIsolateMessage;

  @override
  Stream<T> get onMessage => _delegate.onMessage;

  @override
  void sendIsolate(message) => _delegate.sendIsolate(message);

  @override
  void sendResult(T result) => _delegate.sendResult(result);
}
