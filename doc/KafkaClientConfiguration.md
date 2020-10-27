# Kafka Client Configuration


## KafkaConsumer Configuration

Item                        |  Description                                                                         | Options             | Default value
----------------------------|--------------------------------------------------------------------------------------|---------------------|----------------
BOOTSTRAP_SERVERS           | List of host:port pairs, comma(,) seperated string. E.g, "host1:port1,host2:port2"   |                     | Mandatory (with no default)
GROUP_ID                    | Consumer's group ID string                                                           |                     | Randomly generated string
CLIENT_ID                   | Kafka Consumer's ID string                                                           |                     | Randomly generated string
AUTO_OFFSET_RESET           | Where it starts to read while doesn't have a valid committed offset                  | latest, earliest    | latest
MAX_POLL_RECORDS            | The maximum number of records that a single call to poll() will return               | Integer[1, ...]     | 500
ENABLE_PARTITION_EOF        | Emit EOF event whenever the consumer reaches the end of a partition                  | true, false         | false
QUEUED_MIN_MESSAGES         | Minimum number of messages per topic/partition tries to maintain in the local consumer queue;<br />A Larger value means more frequently to send FetchRequest toward brokers | Integer[1, 10000000] | 100000
SESSION_TIMEOUT_MS          | Client group session and failure detection timeout;<br />If no heartbeat received by the broker, the consumer would be removed from the consumer group                      | Integer[1, 3600000]  | 10000
SOCKET_TIMEOUT_MS           | Timeout for network requests (i.e, OffsetCommitRequest)                              | Integer[10, 300000] | 60000
SECURITY_PROTOCOL           | Protocol used to communicate with brokers                                            | plaintext, ssl, sasl_palintest, sasl_ssl | plaintext
SASL_KERBEROS_KINIT_CMD     | Shell command to refresh or acquire the client's Kerberos ticket                     |                     |
SASL_KERBEROS_SERVICE_NAME  | The client's Kerberos principal name                                                 |                     |


## KafkaProducer Configuration

Item                            |  Description                                                                                               | Options                   | Default value
--------------------------------|------------------------------------------------------------------------------------------------------------|---------------------------|----------------
BOOTSTRAP_SERVERS               | List of host:port pairs, comma(,) seperated string. E.g, "host1:port1,host2:port2"                         |                           | Mandatory (with no default)
CLIENT_ID                       | Kafka Producer's ID string                                                                                 |                           | Randomly generated string
ACKS                            | How many partition replicas must receive the record before the producer can consider the write successful  | 0, 1, all(-1)             | all
QUEUE_BUFFERING_MAX_MESSAGES    | Maximum number of messages allowed on the producer queue                                                   | Integer[1, 10000000]      | 100000
QUEUE_BUFFERING_MAX_KBYTES      | Maximum total message size sum allowed on the producer queue                                               | Integer[1, INT_MAX]       | 0x100000 (1GB)
LINGER_MS                       | Delay in milliseconds to wait before constructing messages batches to transmit to brokers                  | Double[0, 900000.0]       | 0 (KafkaSyncProducer);<br />5 (KafkaAsyncProducer)
BATCH_NUM_MESSAGES              | Maximum number of messages batched in one messageSet                                                       | Integer[1, 1000000]       | 10000
MESSAGE_MAX_BYTES               | Maximum Kafka protocol request message (or batch) size                                                     | Integer[1000, 1000000000] | 1000000
MESSAGE_TIMEOUT_MS              | This value is enforced locally and limits the time a produced message waits for successful delivery        | Integer[0, INT32_MAX]     | 300000
REQUEST_TIMEOUT_MS              | This value is only enforced by the brokers and relies on `ACKS` being non-zero;<br /> It indicates how long the leading broker should wait for in-sync replicas to acknowledge the message. | Integer[1, 900000] | 30000
PARTITIONER                     | The default partitioner for a ProducerRecord (with no partition assigned).<br />Note: partitioners with postfix `_random` mean `ProducerRecord`s with empty key are randomly partitioned.  | random,<br />consistent, consistent_random,<br />murmur2, murmur2_random,<br />fnv1a, fnv1a_random | murmur2_random
MAX_IN_FLIGHT                   | Maximum number of in-flight requests per broker connection                                                 | Integer[1, 1000000]       | 1000000 (while `enable.idempotence`=false);<br />5 (while `enable.idempotence`=true)
ENABLE_IDEMPOTENCE              | This feature ensures that messages are successfully sent exactly once and in the original order            | true, false               | false
SECURITY_PROTOCOL               | Protocol used to communicate with brokers                                                                  | plaintext, ssl, sasl_palintest, sasl_ssl | plaintext
SASL_KERBEROS_KINIT_CMD         | Shell command to refresh or acquire the client's Kerberos ticket                                           |                           |
SASL_KERBEROS_SERVICE_NAME      | The client's Kerberos principal name                                                                       |                           |


## References

* [librdkafka configuration](https://docs.confluent.io/current/clients/librdkafka/md_CONFIGURATION.html)

* [Java's ConsumerConfig](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/consumer/ConsumerConfig.html)

* [Java's ProducerConfig](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/ProducerConfig.html)

