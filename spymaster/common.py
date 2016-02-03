class Metric(object):
    def __init__(self, emitter, key, **attributes):
        self._emitter = emitter
        self.key = key
        self.attributes = attributes

    def with_attrs(self, **attributes):
        return Metric(self._emitter, self.key,
                      **dict(self.attributes.items() + attributes.items()))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def emit(self, value, **attributes):
        self._emitter({'type': 'metric',
                       'key': self.key,
                       'value': value,
                       'attributes': (dict(self.attributes.items() + attributes.items())
                                      if attributes else self.attributes)})


class Collector(object):
    DEFAULT_CONFIG = {}

    def __init__(self, config):
        self.config = dict(self.DEFAULT_CONFIG)
        self.config.update(config)

