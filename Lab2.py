import os
import asyncio
from threading import Thread, Condition, Lock, Semaphore
from typing import TypeVar, Generic, Callable, Type, List
from queue import SimpleQueue
import time
import weakref
from datetime import datetime

PENDING = 'PENDING'
FINISHED = 'FINISHED'

_FUTURE_STATES = [
    PENDING,
    FINISHED,
]


class FutureResult:

    def __init__(self):
        self._condition = Condition()
        self._result = None
        self._exception = None
        self._state = PENDING

    def setResult(self, result):
        with self._condition:
            if self._state in {FINISHED}:
                raise asyncio.InvalidStateError('{}: {!r}'.format(self._state, self))
            self._result = result
            self._state = FINISHED
            self._condition.notify_all()

    def _result(self):
        if self._exception:
            try:
                raise self._exception
            finally:
                self = None
        else:
            return self._result

    def set_exception(self, exception):
        with self._condition:
            if self._state in {FINISHED}:
                raise asyncio.InvalidStateError('{}: {!r}'.format(self._state, self))
            self._exception = exception
            self._state = FINISHED
            self._condition.notify_all()

    def result(self):
        try:
            with self._condition:
                if self._state == FINISHED:
                    return self._result

                self._condition.wait()

                if self._state == FINISHED:
                    return self._result
        finally:
            self = None


class WorkItem:

    def __init__(self, func, future, args, kwargs):
        self.future = future
        self.func = func
        self.args = args
        self.kwargs = kwargs


class WorkerThread(Thread):

    def __init__(self, queue=None, args=(), kwargs=None):

        if queue is None:
            raise ValueError("queue not provided")

        self._queue = queue
        self._executor_reference = args
        super(WorkerThread, self).__init__(args=args, kwargs=kwargs)

    def run(self):
        try:
            while True:
                work_item = self._queue.get(block=True)
                if work_item is not None:
                    try:
                        res = work_item.func(*work_item.args, **work_item.kwargs)
                    except BaseException as exc:
                        self.future.set_exception(exc)
                        self = None
                    else:
                        work_item.future.setResult(res)

                    continue

                executor = self._executor_reference()
                if executor is None or executor._shutdown:
                    if executor is not None:
                        executor._shutdown = True
                    self._queue.put(None)
                    return
                del executor
        except BaseException:
            print('Exception in worker')


class CustomExecutor:
    def __init__(self, max_workers=None):

        if max_workers is None:
            max_workers = min(32, (os.cpu_count() or 1))

        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._max_workers = max_workers
        self._queue = SimpleQueue()
        self._shutdown = False
        self._shutdown_lock = Lock()
        self._threads = set()

    def execute(self, func, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

        future = FutureResult();
        work_item = WorkItem(func, future, args, kwargs)
        self._queue.put(work_item)
        self._count_thread()

        return future

    def _count_thread(self):

        def cb(_, q=self._queue):
            q.put(None)

        threads_count = len(self._threads)
        if threads_count < self._max_workers:
            t = WorkerThread(queue=self._queue, args=(weakref.ref(self, cb)))
            t.start()
            self._threads.add(t)

    def shutdown(self):
        with self._shutdown_lock:
            self._shutdown = True
            self._queue.put(None)
        for t in self._threads:
            t.join()

    def map(self, func, *iter):
        fs = [self.execute(func, *args) for args in zip(*iter)]

        def iterator():
            fs.reverse()
            while fs:
                yield fs.pop()

        return iterator()

def longRunningTask(x):
    time.sleep(2)
    return x ** 2


executer_1 = CustomExecutor(max_workers=1)
print("Example with 1 workers")
futures = executer_1.map(longRunningTask, [1, 2, 3, 4])
for f in futures:
    print(f.result())
    print("Current Time =", datetime.now().strftime("%H:%M:%S"))
executer_1.shutdown()

print("---------------------------------")
executer_2 = CustomExecutor(max_workers = 2)
future_obj = executer_2.execute(longRunningTask, 2)
print("Example with 2 workers")
futures = executer_2.map(longRunningTask, [1,2,3,4])
for f in futures:
    print(f.result())
    print("Current Time =", datetime.now().strftime("%H:%M:%S"))
executer_2.shutdown()

print("---------------------------------")
executer_3 = CustomExecutor(max_workers=4)
print("Example with 4 workers")
futures = executer_3.map(longRunningTask, [1, 2, 3, 4])
for f in futures:
    print(f.result())
    print("Current Time =", datetime.now().strftime("%H:%M:%S"))
executer_3.shutdown()