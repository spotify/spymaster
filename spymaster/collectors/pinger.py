import urllib2
from collections import namedtuple
from urlparse import urlparse
from spymaster.common import Collector


_Target = namedtuple('_Target', ['service', 'hostname',
                                 'url', 'expected_status',
                                 'timeout'])


class Pinger(Collector):
    def __init__(self, config):
        super(Pinger, self).__init__(config)

        # TODO: Make configurable
        self.default_timeout = 5

        targets = []
        for service, target in self.config.get('targets', {}).iteritems():
            url = urlparse(target['url'])
            targets.append(_Target(service=service,
                                   hostname=url.hostname,
                                   url=url.geturl(),
                                   expected_status=target.get('expected_status'),
                                   timeout=target.get('timeout', self.default_timeout)))
        self.targets = targets

    def __call__(self, context):
        with context.create_metric(key='pinger',
                                   what='time_since_last_OK',
                                   unit='s') as m:
            for target in self.targets:
                last_ok = context.state.setdefault(target.url, context.time.now)
                try:
                    resp = urllib2.urlopen(target.url,
                                           timeout=target.timeout)
                    if target.expected_status:
                        if resp.status == target.expected_status:
                            last_ok = context.time.now
                    else:
                        if resp.status / 100 == 2:
                            last_ok = context.time.now
                except urllib2.URLError as e:
                    pass
                m.emit(value=context.time.now - last_ok,
                       pinged_url=target.url,
                       pinged_host=target.hostname)
