import 'dart:async';

import '../isolate_contactor_controller.dart';

/// States of current isolate
enum ComputeState { computing, computed }

/// Port for sending message
enum IsolatePort { main, isolate }

/// Isolate state
enum IsolateState { dispose }

class IsolateContactorException implements Exception {
  final String message;

  IsolateContactorException(this.message);

  @override
  String toString() {
    return 'IsolateContactorException: $message';
  }
}

/// Create a static function to compunicate with main `Isolate`
void internalIsolateFunction(dynamic params) {
  final controller = IsolateContactorController(params, onDispose: () {});
  final function = controller.initialParams;
  controller.onIsolateMessage.listen((message) {
    Completer completer = Completer();
    completer.future.then((value) => controller.sendResult(value));
    completer.complete(function(message));
  });
}
