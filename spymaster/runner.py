import json
from multiprocessing import Process, Event
from spymaster.common import Metric
import signal
import time


class TimeContext(object):
    def __init__(self):
        self.now = None
        self.last = None
        self.delta = None

    def update(self):
        self.last, self.now = self.now, time.time()
        if self.last is not None:
            self.delta = self.now - self.last


class Context(object):
    def __init__(self, state, emitter, timecontext):
        self.state = state
        self._emitter = emitter
        self.time = timecontext

    def create_metric(self, key, **attributes):
        return Metric(self._emitter, key, **attributes)


class Task(object):
    def __init__(self, collector, interval):
        self.interval = interval
        self.collector = collector
        self.state = {}
        self.timecontext = TimeContext()

    def run_once(self):
        metrics = []
        context = Context(state=self.state,
                          emitter=metrics.append,
                          timecontext=self.timecontext)
        self.timecontext.update()
        self.collector(context)
        return metrics

    def __call__(self, shutdown_event):
        while True:
            try:
                metrics = self.run_once()
                # TODO: Do something useful with the metrics
                print(metrics)
            except Exception as e:
                # TODO: Implement Collector initiated backoff
                print e
            if shutdown_event.wait(self.interval):
                break


class TaskManager(object):
    DEFAULT_INTERVAL = 5
    DEFAULT_MODULENAME = 'spymaster.collectors'
    DEFAULT_COLLECTORCLASS = 'Collector'

    def __init__(self, config):
        self.tasks = []
        modules = {}
        for item in config['collectors']:
            # TODO: Move import to Task after forking
            # XXX: How to validate configuration early, if import happens after forking?
            modulename = item.get('import', self.DEFAULT_MODULENAME)
            collectorclass = item.get('collector', self.DEFAULT_COLLECTORCLASS)

            if modulename not in modules:
                print('Importing {}'.format(modulename))
                modules[modulename] = __import__(modulename, fromlist=[collectorclass])
            else:
                print('{} already imported'.format(modulename))

            Collector = getattr(modules[modulename], collectorclass)
            self.tasks.append(Task(collector=Collector(item.get('config')),
                                   interval=item.get('interval', self.DEFAULT_INTERVAL)))

    def run(self):
        default_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        shutdown_event = Event()
        processes = [Process(target=task,
                             kwargs={'shutdown_event': shutdown_event})
                     for task in self.tasks]

        print "Starting up"
        map(Process.start, processes)

        signal.signal(signal.SIGINT, default_handler)

        try:
            signal.pause()
        except KeyboardInterrupt:
            print "Shutting down"
            shutdown_event.set()
            map(Process.join, processes)


if __name__ == '__main__':
    config = json.load(open('config.json'))
    TaskManager(config).run()
