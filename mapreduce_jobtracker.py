
import re
import urllib2
from mapreduce_utils import MapreduceBase, CollectdMock, CollectdValuesMock

PLUGIN = "mapreduce_jobtracker"

PORT = 50030
HOST = "localhost"

ENABLE_POOL_METRICS = True
ENABLE_JOB_METRICS = True
ENABLE_JOBTRACKER_METRICS = True
VERBOSE_LOGGING = True

POOL_METRICS = dict([(x, "gauge.mapred.scheduler.pools.%s.%s" % ("%s", x)) for x in [
    "demand",
    "fairShare",
    "millisSinceAtHalfFairShare",
    "millisSinceAtMinShare",
    "minShare",
    "runningTasks"
]])

JOB_METRICS = dict([(x, "gauge.mapred.scheduler.jobs.%s.%s" % ("%s", x)) for x in [
    "demand",
    "fairShare",
    "minShare",
    "runningTasks"
    "weight"
]])

JOBTRACKER_METRICS = {
    "maps_launched": "counter.mapred.jobtracker.maps.launched",
    "maps_completed": "counter.mapred.jobtracker.maps.completed",
    "maps_failed": "counter.mapred.jobtracker.maps.failed",
    "maps_killed": "counter.mapred.jobtracker.maps.killed",
    "blacklisted_maps": "counter.mapred.jobtracker.maps.blacklisted",
    "running_maps": "gauge.mapred.jobtracker.maps.running",
    "waiting_maps": "gauge.mapred.jobtracker.maps.waiting",

    "reduces_launched": "counter.mapred.jobtracker.reduces.launched",
    "reduces_completed": "counter.mapred.jobtracker.reduces.completed",
    "reduces_failed": "counter.mapred.jobtracker.reduces.failed",
    "reduces_killed": "counter.mapred.jobtracker.reduces.killed",
    "blacklisted_reduces": "counter.mapred.jobtracker.reduces.blacklisted",
    "running_reduces": "gauge.mapred.jobtracker.reduces.running",
    "waiting_reduces": "gauge.mapred.jobtracker.reduces.waiting",
    
    "jobs_submitted": "counter.mapred.jobtracker.jobs.submitted",
    "jobs_completed": "counter.mapred.jobtracker.jobs.completed",
    "jobs_failed": "counter.mapred.jobtracker.jobs.failed",
    "jobs_killed": "counter.mapred.jobtracker.jobs.killed",
    "jobs_preparing": "gauge.mapred.jobtracker.jobs.preparing",
    "jobs_running": "gauge.mapred.jobtracker.jobs.running",
    
    "map_slots": "gauge.mapred.jobtracker.slots.map",
    "reduce_slots": "gauge.mapred.jobtracker.slots.reduce",
    "occupied_map_slots": "gauge.mapred.jobtracker.slots.map.occupied",
    "occupied_reduce_slots": "gauge.mapred.jobtracker.slots.reduce.occupied",
    

    "trackers": "gauge.mapred.jobtracker.trackers",
    "trackers_blacklisted": "gauge.mapred.jobtracker.trackers.blacklisted",
    "trackers_decommissioned": "gauge.mapred.jobtracker.trackers.decommissioned",

    "heartbeats": "counter.mapred.jobtracker.heartbeats"
}

class MapreduceJobtracker(MapreduceBase):

    def __init__(self, collectd):
        super(MapreduceJobtracker, self).__init__(PLUGIN, HOST, PORT, VERBOSE_LOGGING, collectd)

        self.enable_pool_metrics = ENABLE_POOL_METRICS
        self.enable_job_metrics = ENABLE_JOB_METRICS
        self.enable_jobtracker_metrics = ENABLE_JOBTRACKER_METRICS

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
            elif node.key == 'EnablePoolMetrics':
                self.enable_pool_metrics = bool(node.values[0])
            elif node.key == 'EnableJobMetrics':
                self.enable_job_metrics = bool(node.values[0])
            elif node.key == 'EnableJobtrackerMetrics':
                self.enable_jobtracker_metrics = bool(node.values[0])
            else:
                collectd.warning('%s plugin: Unknown config key: %s.'
                                 % (self.plugin, node.key))

    
    def handle_metrics(self, path, value):
        if len(path) < 2:
            return

        if self.enable_pool_metrics and path[1] == "pools" and path[-1] in POOL_METRICS:
            self.dispatch_fairscheduler_stat(path, value, POOL_METRICS)
            return
    
        if self.enable_job_metrics and path[1] == "jobs" and path[-1] in JOB_METRICS:
            self.dispatch_fairscheduler_stat(path, value, JOB_METRICS)
            return
    
        if self.enable_jobtracker_metrics and path[1] == "jobtracker" and path[-1] in JOBTRACKER_METRICS:
            self.dispatch_jobtracker_stat(path, value)
            return
    
    
    def dispatch_fairscheduler_stat(self, path, value, metrics_dict):
        parent = path[-2]
        coords = dict([x.split('=') for x in parent[1:-2].split(',')])
    
        key = metrics_dict[path[-1]] % coords['taskType'].lower()
        plugin = "mapred_%s_%s" % (path[0], path[1])
        plugin_instance = coords['name']
    
        self.dispatch_stat(key, value, plugin, plugin_instance)
    
    
    def dispatch_jobtracker_stat(self, path, value):
        key = JOBTRACKER_METRICS[path[-1]]
        plugin = "mapred_jobtracker"
        plugin_instance = "jobtracker"
    
        self.dispatch_stat(key, value, plugin, plugin_instance)


if __name__ == '__main__':
    import sys
    collectd = CollectdMock(PLUGIN)
    jt = MapreduceJobtracker(collectd)
    jt.fetch_stats(jt.handle_metrics)
else:
    import collectd
    jt = MapreduceJobtracker(collectd)
    collectd.register_config(jt.configure_callback)
    collectd.register_read(jt.read_callback)
