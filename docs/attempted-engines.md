# Attempted engines

These entries record unsuccessful benchmark attempts, their measured limits and untested options.
They have no published result.

## Kafka Connect with the Iceberg sink

**Track:** [`events-1mrps-skew`](../workloads/tracks/events-1mrps-skew.yaml) (1376 MB/s, 1 M rows/s, 200 columns,
512 skewed keys, 192 Kafka partitions, round-robin assignment, freshness p95 90 s and max 120 s).
**Verdict:** the tested configuration failed to keep up. It committed about three quarters of the offer between
snapshots, with a four-minute commit gap. Further tuning remains untested.

### Sustain attempt (2026-10-23)

The run used Kafka Connect 4.3.2, the unmodified Iceberg Kafka Connect sink 1.12.0, Confluent Avro converter 8.3.2
and harness 1a943d4:

- 192 sink tasks, the active-task limit for 192 Kafka partitions, on 48 workers with 4 tasks each.
- Each worker had 6 vCPU, 40 GiB memory and a 32 GiB heap, spread across 10 r8g.8xlarge nodes.
- Commit interval 60 seconds, commit timeout 120 seconds, the full track offer and the live gate.
- Control-channel debug logging was enabled to trace commit requests.

| Run ID | Committed rows/s | Commit gap |
|---|---:|---:|
| `kc-s2-20261023T173955Z` | 714,000 | 242 s |

The rate is the rows added by the second snapshot divided by the time since the first. The run produced only those
two snapshots, so this is a single-interval measurement, not an established sustainable throughput ceiling. The
commit gap exceeded the 120-second freshness maximum. Commits alternated: each successful commit was followed by one
that received no replies before its timeout. The coordinator keeps replies that arrive late, so the next commit
carried them and each successful commit held 98,304 files.

The gate stopped the run about 8 minutes after the offer began. It first reported `UNDERSIZED` as the backlog floor
rose from 32 M to 153 M rows over three minutes, then reported `VOID` because scorer samples were stale. The
producers held their schedule throughout, so the offer was real. The run did not complete the hour or establish
final exactly-once correctness.

### Observed bottlenecks

- **Per-task rate.** Keeping up with 192 tasks needs about 5,200 rows/s per task. The sustain run averaged
  about 3,700 rows/s per task between its two snapshots, and the capacity probes 3,500–3,600 at 60-second commits,
  where the control topic was small.
- **File fan-out.** Round-robin input exposes each task to all 512 table partitions. Each successful commit added
  98,304 files (192 × 512), mostly far below the 512 MiB target. In the earlier capacity probes, closing the 512
  files serially took 25–35 seconds per task during a 60-second commit cycle.
- **Delayed commit requests.** Each task reads and decodes every event on the single-partition control topic,
  including other tasks' replies, before handling the next commit request. See the sink's
  [`Channel.consumeAvailable`](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Channel.java)
  and [`Worker.receive`](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/Worker.java).
  The first successful commit grew the control log by about 287 MB. All 192 tasks received the next
  request 155–189 seconds after it was sent, beyond the 120-second timeout. That commit received no replies in time.
  Because every task reads every reply, control traffic grows with the square of the task count.
- **Small control fetches.** The control consumer used the default `max.partition.fetch.bytes=1048576`, below the
  roughly 1.5 MB size of a task reply. Kafka permits an oversized first batch so consumption can progress.
  This setting and the sink's nonblocking polls are a plausible contributor to the delay, but their contribution
  was not isolated by an A/B test. See [Kafka's fetch configuration](https://kafka.apache.org/43/configuration/consumer-configs/#max.partition.fetch.bytes).
- **Memory.** Every worker reached its 32 GiB heap, with two to eight full collections each (median three) and about
  10% of wall time in garbage collection. Workers used about 4.8 of 6 cores over the offer, about one core per
  single-threaded task.

### Why the sink fits this workload poorly

Kafka Connect as a framework fixes which records each task sees: a task owns whole Kafka partitions and writes only
what its own consumer read. The stock Iceberg sink decides how a task writes and commits those records. The measured
limits come from that combination with this workload. They do not show that Kafka Connect cannot ingest 1 M rows/s in
general.

- **Adding tasks multiplies writers.** Connect gives each task whole Kafka partitions, so tasks cannot exceed the
  192 partitions. It delivers each record only to the task that read it, and tasks cannot exchange records, so with
  round-robin input every task writes all 512 partitions. The stock sink keeps all 512 files open at once
  ([`PartitionedAppendWriter`](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/PartitionedAppendWriter.java)
  extends Iceberg's fan-out writer), so more tasks bring more open files, small files and metadata along with more
  processing.
- **Closing files stops ingestion.** The sink handles commit requests in the same call that accepts records. A commit
  request completes the task's writers, and the fan-out writer closes its files one after another
  ([`SinkWriter.completeWrite`](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/SinkWriter.java)).
  The task accepts no records meanwhile, and extra cores do not help a single task thread.
- **The control protocol multiplies the metadata.** Every task decodes every other task's reply before discarding it,
  so fleet-wide decoding grows with the square of the task count. Larger fetches can cut fetch round trips but not
  that duplicated work.
- **Freshness blocks the usual trade.** Longer commit intervals would amortize file closing and produce larger files,
  but this track also requires p95 90 s and max 120 s freshness. Shorter intervals add closing and coordination work.
- **Memory follows from the fan-out.** An open file buffers its rows until a 128 MiB row group fills or the file
  closes. These files never reach that size, so each holds every row since the last commit, and the heap grows with
  the commit gap and with open files per task. The sustain run held every heap at the limit.
- **The output is costly even at full speed.** At the four-minute commit gap measured, the median file was 0.27 MB and
  90% were under 1 MiB. At 60-second commits, 98,304 files per commit would be about 5.9 M files an hour, each about
  a quarter of that size, which shifts a large cost to compaction and queries.

The measurements fit these mechanisms: about 3,500 rows/s per task with timely commits in the probes, then commit delays
from the control topic at full scale. Passing would likely require a sink that closes files off the task thread and
exchanges commit metadata without every task decoding every reply, or a different record assignment or table layout,
which changes the track. No stock configuration was shown to be impossible.

### Untested options

Increasing `iceberg.kafka.max.partition.fetch.bytes` could reduce the control delay. More heap or a different commit
interval could also change throughput, but any candidate must still meet the freshness limits. None was tested.
The 192-partition limit caps active tasks, not per-task throughput, so these runs do not establish that tuning
cannot meet the offer. Changing Kafka partition count, record assignment, table layout or column count changes
this track. Assigning records by key would also leave one task with the hottest key's roughly 15% of rows.

### Capacity probes (2026-10-16)

Two 10-minute [saturation](saturation.md) probes on harness 2fa57cb and Kafka Connect 4.3.1 used 4 tasks per worker,
6 vCPU and 40 GiB per worker, a 32 GiB heap and 60-second commits. Both passed exactly-once validation.

| Fleet | Tasks | Committed rows/s | Files per commit | Median file |
|---|---:|---:|---:|---:|
| 1 worker | 4 | 14,326 | 2,048 | 0.1 MiB |
| 2 workers | 8 | 27,795 | 4,096 | 0.1 MiB |

Rates are commit to commit under overload, not sustainable results.

### Integration notes

- **Version compatibility:** Kafka Connect 4.4.0 moved `AppInfoParser` to `org.apache.kafka.common.utils.internals`,
  and the 1.12.0 sink fails at task start with `NoClassDefFoundError`. Use Kafka Connect 4.3.x, which runs the sink
  unmodified.
- **Control messages:** replies exceeded Kafka's default 1 MB limit. The runs set `iceberg.kafka.max.request.size`
  and the control topic's `max.message.bytes` to 8 MiB. These producer/topic limits do not enlarge consumer fetches.
- **Commit timeout:** the default `iceberg.control.commit.timeout-ms=30000` was too short for the probes.
  The sustain attempt used 120 seconds and still timed out.
- **Schema registry:** the pinned [Apicurio 3.0.15](../deploy/k8s/schema-registry.yaml.tmpl) rejected the Avro
  converter's schema-version lookup for `events200`. The runs used Apicurio 3.3.3, with health checks on port 9000.
- **Worker setup:** set the heap explicitly, use `offset.flush.interval.ms=1000` to shorten idle polling delays,
  and pre-create the worker's internal topics and control topic because the harness disables topic auto-creation.
