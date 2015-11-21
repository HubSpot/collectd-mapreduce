# Mapreduce CollectD plugin

A [CollectD](http://collectd.org) plugin to collect MapReduce stats and metrics. Uses CollectD's [Python plugin](http://collectd.org/documentation/manpages/collectd-python.5.shtml).

Tested on CDH4 and CDH5 JobTrackers and TaskTrackers.
Requires Collectd

### Prepration

Hadoop metrics should be enabled. This is done by editing `/etc/hadoop/conf/hadoop-metrics.properties`.  
For each class listed in the file, change to `org.apache.hadoop.metrics.spi.NoEmitMetricsContext`.  
Restart the jobtracker or tasktracker to pick up new configuration.  
Test by going to `http://hostname:port/metrics` for the hostname and port of your jobtracker/tasktracker.

### Installation

1. Ensure hadoop-metrics is enabled.
2. Place all three .py files in /usr/share/collectd/python/collectd-mapreduce/
3. Place mapreduce-jobtracker.conf and/or mapreduce-tasktracker.conf in your `/etc/collectd.d/` or equivalent configuration directory.
4. Restart collectd
