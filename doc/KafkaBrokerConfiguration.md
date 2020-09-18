# Kafka Broker Configuration

## Java Version

* Recommend the latest released version of JDK 1.8, -- LinkedIn is currently running JDK 1.8 u5.

## JVM Configuration

* Here is a sample for `KAFKA_JVM_PERFORMANCE_OPTS`

    -Xmx8g -Xms8g -XX:MetaspaceSize=96m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:G1HeapRegionSize=16M -XX:MinMetaspaceFreeRatio=50 -XX:MaxMetaspaceFreeRatio=80

## Deployment

* In at least three data centers with high bandwidth and low latency between them. (Commonly, using three availability zones inside one region of a cloud provider)

    * IMPORTANT: the `latency/bandwidth` between brokers could highly impact the `throughput/latency` of a producer client.

* `rack.id` could be used to identify brokers from different data centers.


## Functionality

* Controller

    * Maintains leaders/replicas info for partitions.

* Partition Replicas

    * Leader replica

        * All produce/consume requests go through the leader.

    * In-Sync Replica

        * Replicas that are continuously asking for the latest messages; has caught up to the most recent message in 10 seconds (`replica.lag.time.max.ms`).

        * The preferred in-sync replica would be promoted to new leader while the previous one fails.

## OS tuning

(Use `sysctl`, or edit `/etc/sysctl.conf` for permanent change)

* File descriptor limits

    * `fs.file-max`

    * Recommended 100000 or higher.

* Maximum number of memory map areas for a process

    * `vm.max_map_count`
    
    * Each log segment uses 2 map areas.

* Virtual memory

    * It's best to avoid swapping at all costs.

    * Set `vm.swappiness` to a very low value (e.g, 1).

* Dirty page

    * `vm.dirty_background_ratio=5`,  is appropriate in many situations.

    * `vm.dirty_ratio=60(~80)`, is a reasonable number.

* Networking

    * `net.core.wmem_default` and `net.core.rmem_default`, reasonable setting: 131072 (128KB).

    * `net.core.wmem_max` and `net.core.rmem_max`, reasonable setting: 2097152 (2MB).

    * `net.ipv4.tcp_wmem` and `net.ipv4.tcp_rmem`, an example setting: 4096 (4KB minimum), 65536 (64KB default), 2048000 (2MB maximum).

    * `net.ipv4.tcp_window_scaling=1`.

    * `net.ipv4.tcp_max_syn_backlog`, should be above 1024 (default) to allow more simultaneous connections.

    * `net.core.netdev_max_backlog`, should be above 1000 (default) to allow more packets to be queued to process.

## Disks and File-system

* Throughput of the broker disks directly influence the performance of producer clients.

* EXT4 and XFS are the most popular choices (XFS with better performance). Some companies are even trying with ZFS.

* Do not use mounted shared drives and any network file systems.

* Do not share the same drives used for Kafka data with other applications to ensure good latency.

## Broker Settings

* Auto-created Topics

    * With `auto.create.topics.enable=true`, a topic could be created while,

        1. A producer starts writing messages to the topic.

        2. A consumer starts reading messages from the topic.

        3. Any client requests metadata for the topic.

    * The auto-created topics might not be what you want

        You might want to override some default configurations

        1. `default.replication.factor=3`

            * We recommend a replication factor of 3 (at least) for any topic where availability is an issue.

            * The replication factor should be no more than the number of brokers.

        2. `offsets.topic.replication.factor=3`

            * It's for the internal topic `__consumer_offsets`, -- auto-topic-creation will fail with a GROUP_COORDINATOR_NOT_AVAILABLE error if the cluster can't meet this replication factor requirement.

        3. `num.partitions=5` (or whatever you want)

    * Unclean leader election

        * Set `unclean.leader.election.enable=false` to avoid out-of-sync replicas.

    * Minimal in-sync replicas

        * Set `min.insync.replicas=2` (at least) for fault-tolerant.

    * Log

        * `log.retention.bytes` and `log.retention.ms/hours`, -- the log segment will be cleared if it exceeds the limits.

        * `log.segment.bytes` and `log.segment.ms`, -- a new log segment will be created if any of the limits is reached.

    * Threads for recovery

        * `num.recovery.threads.per.data.dir` (default 1), could be a larger number to speed up opening/closing log segments, recovering from failure, etc.

    * Maximum message size supported

        * `message.max.bytes` (default 1000012).

        * `replica.fetch.max.bytes` MUST be larger than `message.max.bytes`.

        * MUST be coordinated with (lower than) the `fetch.message.max.bytes` configuration of consumer clients.

        * MUST be coordinated (same) with the `message.max.bytes` configuration of producer clients.

# Performance tips

* Factors: Memory, Disk, Partitions, and Ethernet bandwidth.

* Partitions could be used to improve throughput, by using multiple Producers/Consumers.

* Suggests that limiting the size of the partition on the disk to less than 6 GB per day of retention often gives satisfactory results.

