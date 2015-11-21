
import re
import urllib2
from mapreduce_utils import MapreduceBase, CollectdMock, CollectdValuesMock

PLUGIN = "mapreduce_tasktracker"

PORT = 50060
HOST = "localhost"

ENABLE_TASKTRACKER_METRICS = True
ENABLE_SHUFFLE_METRICS = True
VERBOSE_LOGGING = True

SHUFFLE_METRICS = {
    "shuffle_exceptions_caught": "counter.mapred.shuffle.exceptions_caught",
    "shuffle_failed_outputs": "counter.mapred.shuffle.output.fails",
    "shuffle_handler_busy_percent": "gauge.mapred.shuffle.handler.busy_percent",
    "shuffle_output_bytes": "counter.mapred.shuffle.output.bytes",
    "shuffle_success_outputs": "counter.mapred.shuffle.output.successes",
}

TASKTRACKER_METRICS = {
    "failedDirs": "gauge.mapred.tasktracker.dirs.failed",
    "mapTaskSlots": "gauge.mapred.tasktracker.slots.map",
    "maps_running": "gauge.mapred.tasktracker.maps.running",
    "reduceTaskSlots": "gauge.mapred.tasktracker.slots.reduce",
    "reduces_running": "gauge.mapred.tasktracker.reduces.running",
    "tasks_completed": "counter.mapred.tasktracker.tasks.completed",
    "tasks_failed_ping": "counter.mapred.tasktracker.tasks.failed.ping",
    "tasks_failed_timeout": "counter.mapred.tasktracker.tasks.failed.timeout",
}

class MapreduceTasktracker(MapreduceBase):

    def __init__(self, collectd):
        super(MapreduceTasktracker, self).__init__(PLUGIN, HOST, PORT, VERBOSE_LOGGING, collectd)

        self.enable_shuffle_metrics = ENABLE_SHUFFLE_METRICS
        self.enable_tasktracker_metrics = ENABLE_TASKTRACKER_METRICS

    # collectd callbacks
    def read_callback(self):
        """called by collectd to gather stats. It is called per collection interval.
        If this method throws, the plugin will be skipped for an increasing amount
        of time until it returns normally again"""
        self.log_verbose('Read callback called')
        self.fetch_stats(self.handle_metrics)
    
    
    def configure_callback(self, conf):
        """called by collectd to configure the plugin. This is called only once"""
        for node in conf.children:
            if node.key == 'Host':
                self.host = node.values[0]
            elif node.key == 'Port':
                self.port = int(node.values[0])
            elif node.key == 'Verbose':
                self.verbose_logging = bool(node.values[0])
            elif node.key == 'EnableShuffleMetrics':
                self.enable_shuffle_metrics = bool(node.values[0])
            elif node.key == 'EnableTasktrackerMetrics':
                self.enable_tasktracker_metrics = bool(node.values[0])
            else:
                collectd.warning('%s plugin: Unknown config key: %s.'
                                 % (self.plugin, node.key))

    
    def handle_metrics(self, path, value):
        if len(path) < 2:
            return

        if self.enable_shuffle_metrics and path[1] == "shuffleOutput" and path[-1] in SHUFFLE_METRICS:
            self.dispatch_tasktracker_stat(path, value, "shuffle", SHUFFLE_METRICS)
            return
    
        if self.enable_tasktracker_metrics and path[1] == "tasktracker" and path[-1] in TASKTRACKER_METRICS:
            self.dispatch_tasktracker_stat(path, value, "tasktracker", TASKTRACKER_METRICS)
            return
    
    
    def dispatch_tasktracker_stat(self, path, value, plugin_instance, metrics_dict):
        key = metrics_dict[path[-1]]
        plugin = "mapred_tasktracker"
    
        self.dispatch_stat(key, value, plugin, plugin_instance)


if __name__ == '__main__':
    import sys
    collectd = CollectdMock(PLUGIN)
    jt = MapreduceTasktracker(collectd)
    jt.fetch_stats(jt.handle_metrics)
else:
    import collectd
    jt = MapreduceTasktracker(collectd)
    collectd.register_config(jt.configure_callback)
    collectd.register_read(jt.read_callback)
