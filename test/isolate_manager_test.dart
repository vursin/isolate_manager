import 'package:isolate_manager/isolate_manager.dart';
import 'package:test/test.dart';

void main() {
  test('Test IsolateManager.create: Basic Usage', () async {
    // Create IsolateContactor
    print('Create IsolateManager instance');
    IsolateManager<int> isolateManager =
        IsolateManager.create(numOfIsolates: 4, isolateFunction: fibonacci);

    print('Starting IsolateManager instance...');
    await isolateManager.start();

    print('Computing IsolateManager instance...');
    final result = await isolateManager.compute(3);

    expect(result, fibonacci(3));

    print('Dispose IsolateManager instance.');
    isolateManager.stop();
  });

  test('Test IsolateManager.create', () async {
    // Create IsolateContactor
    print('Create IsolateManager instance');
    IsolateManager<int> isolateManager =
        IsolateManager.create(numOfIsolates: 4, isolateFunction: fibonacci);

    print('Starting IsolateManager instance...');
    await isolateManager.start();

    print('Computing IsolateManager instance...');
    await Future.wait([
      for (int i = 0; i < 10; i++)
        isolateManager.compute(i).then((value) {
          print('Fibonacci $i = $value');
          expect(value, fibonacci(i));
        })
    ]);

    await Future.delayed(Duration(seconds: 3));

    print('Dispose IsolateManager instance.');
    isolateManager.stop();
  });

  test('Test IsolateManager.createOwnIsolate', () async {
    // Create IsolateContactor
    print('Create IsolateManager instance');
    IsolateManager<int> isolateManager = IsolateManager.createOwnIsolate(
      numOfIsolates: 4,
      isolateFunction: isolateFunction,
    );

    isolateManager.stream.listen((value) {
      print('Stream: $value');
    });

    print('Starting IsolateManager instance...');
    await isolateManager.start();

    print('Computing IsolateManager instance...');
    await Future.wait([
      for (int i = 0; i < 10; i++)
        isolateManager.compute(i).then((value) {
          print('Fibonacci $i = $value');
          expect(value, fibonacci(i));
        })
    ]);
    await Future.delayed(Duration(seconds: 3));

    print('Restarting IsolateManager instance...');
    await isolateManager.restart();

    print('Computing IsolateManager instance second times...');
    await Future.wait([
      for (int i = 5; i < 13; i++)
        isolateManager.compute(i).then((value) {
          print('Fibonacci $i = $value');
          expect(value, fibonacci(i));
        })
    ]);

    await Future.delayed(Duration(seconds: 3));

    print('Dispose IsolateManager instance.');
    await isolateManager.stop();
  });
}

int fibonacci(dynamic n) {
  if (n <= 2) return 1;

  int n1 = 0, n2 = 1, n3 = 1;

  for (int i = 2; i <= n; i++) {
    n3 = n1 + n2;
    n1 = n2;
    n2 = n3;
  }

  return n3.round();
}

void isolateFunction(dynamic params) {
  final controller = IsolateManagerController(params);

  controller.onIsolateMessage.listen((message) {
    final result = fibonacci(message);

    controller.sendResult(result);
  });
}
