import urllib2
import re

class MapreduceBase(object):
    def __init__(self, plugin, host, port, verbose_logging, collectd):
        super(MapreduceBase, self).__init__()
        self.plugin = plugin
        self.host = host
        self.port = port
        self.verbose_logging = verbose_logging
        self.collectd = collectd

    def fetch_stats(self, handler):
        """
        fetches all required stats from the metrics endpoint.
        """
    
        response = self.fetch_url("http://%s:%d/metrics" % (self.host, self.port))
        path = []
        last_index = -1

        for line in response.readlines():
            m = re.match(r"^(\s+)?(.*)$", line)
            indent, key = m.groups()
            
            parts = key.split('=')
            if len(parts) == 2:
                key = parts[0]
                value = float(parts[1])
            else:
                value = None

            # tab-spacing is 2. get this down to normal array indexes 0-indexed
            index = (len(indent) if indent else 0)/2 

            if index > last_index:
                path.append(key)
            else:
                if index < last_index:
                    path = path[0:index+1]
                path[index] = key
            
            last_index = index

            handler(path, value)

    def dispatch_stat(self, name, value, plugin, plugin_instance):
        """Read a key from info response data and dispatch a value"""
    
        if value is None:
            self.collectd.warning('%s plugin: Value not found for %s' % (self.plugin, name))
            return
    
        data_type, type_instance = name.split(".", 1)
        self.log_verbose('Sending value[%s]: %s=%s' % (data_type, name, value))
    
        val = self.collectd.Values(plugin=plugin)
        val.plugin_instance = plugin_instance
        val.type = data_type
        val.type_instance = type_instance
        val.values = [value]
        val.meta = {'0': True}
        val.dispatch()
    
    def fetch_url(self, url):
        self.log_verbose(url)
        try:
            return urllib2.urlopen(url, timeout=10)
        except urllib2.URLError, e:
            self.collectd.error(
                '%s plugin: Error connecting to %s - %r' % (self.plugin, url, e))
            return None

    def log_verbose(self, msg):
        if not self.verbose_logging:
            return
        self.collectd.info('%s plugin [verbose]: %s' % (self.plugin, msg))

# The following classes are there to launch the plugin manually
# with something like ./mapreduce_jobtracker.py for development
# purposes. They basically mock the calls on the "collectd" symbol
# so everything prints to stdout.
class CollectdMock(object):

    def __init__(self, plugin):
        self.value_mock = CollectdValuesMock
        self.plugin = plugin

    def info(self, msg):
        print 'INFO: %s' % (msg)

    def warning(self, msg):
        print 'WARN: %s' % (msg)


    def error(self, msg):
        print 'ERROR: %s' % (msg)
        sys.exit(1)

    def Values(self, plugin=None):
        return (self.value_mock)()


class CollectdValuesMock(object):

    def dispatch(self):
        print self

    def __str__(self):
        attrs = []
        for name in dir(self):
            if not name.startswith('_') and name is not 'dispatch':
                attrs.append("%s=%s" % (name, getattr(self, name)))
        return "<CollectdValues %s>" % (' '.join(attrs))

