Kafka Log Splitter
==================

Negative offsets in Log Segment Index files due to Integer overflow when compaction is enabled
(https://issues.apache.org/jira/browse/KAFKA-3323)

Cleaner can generate unindexable log segments
(https://issues.apache.org/jira/browse/KAFKA-2024)

In versions of Kafka < 0.9.0 it was possible for the LogCleaner to group logs for compaction such that they 
were impossible to index correctly.  Such logs would be silently indexed incorrectly, and reads on them will
skip offset intervals.  Once the log file is affected, re-indexing will not fix the problem. This utility splits 
log files into legally sized chunks, so that they can be successfully indexed.

Build with maven:

`mvn clean package`

Run:

`java -jar kafka-logsplitter-1.0.0-SNAPSHOT.jar /kafka/attainment_event-4`


