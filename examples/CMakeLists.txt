project("kafka-examples")


# Target: kafka_sync_producer
add_executable("kafka_sync_producer" "kafka_sync_producer.cc")
target_link_libraries("kafka_sync_producer" modern-cpp-kafka-api)


# Target: kafka_async_producer_copy_payload
add_executable("kafka_async_producer_copy_payload" "kafka_async_producer_copy_payload.cc")
target_link_libraries("kafka_async_producer_copy_payload" modern-cpp-kafka-api)


# Target: kafka_async_producer_not_copy_payload
add_executable("kafka_async_producer_not_copy_payload" "kafka_async_producer_not_copy_payload.cc")
target_link_libraries("kafka_async_producer_not_copy_payload" modern-cpp-kafka-api)


# Target: kafka_auto_commit_consumer
add_executable("kafka_auto_commit_consumer" "kafka_auto_commit_consumer.cc")
target_link_libraries("kafka_auto_commit_consumer" modern-cpp-kafka-api)


# Target: kafka_manual_commit_consumer
add_executable("kafka_manual_commit_consumer" "kafka_manual_commit_consumer.cc")
target_link_libraries("kafka_manual_commit_consumer" modern-cpp-kafka-api)


# Target: example_Properties
add_executable("example_Properties" "example_Properties.cc")
target_link_libraries("example_Properties" modern-cpp-kafka-api)

# Target: example_KafkaProducer_Simple
add_executable("example_KafkaProducer_Simple" "example_KafkaProducer_Simple.cc")
target_link_libraries("example_KafkaProducer_Simple" modern-cpp-kafka-api)

# Target: example_KafkaProducer_Lifecycle
add_executable("example_KafkaProducer_Lifecycle" "example_KafkaProducer_Lifecycle.cc")
target_link_libraries("example_KafkaProducer_Lifecycle" modern-cpp-kafka-api)

# Target: example_KafkaProducer_DeepCopy
add_executable("example_KafkaProducer_DeepCopy" "example_KafkaProducer_DeepCopy.cc")
target_link_libraries("example_KafkaProducer_DeepCopy" modern-cpp-kafka-api)

# Target: example_KafkaProducer_EnableManualEventsPoll
add_executable("example_KafkaProducer_EnableManualEventsPoll" "example_KafkaProducer_EnableManualEventsPoll.cc")
target_link_libraries("example_KafkaProducer_EnableManualEventsPoll" modern-cpp-kafka-api)

# Target: example_ProducerRecordHeaders
add_executable("example_ProducerRecordHeaders" "example_ProducerRecordHeaders.cc")
target_link_libraries("example_ProducerRecordHeaders" modern-cpp-kafka-api)

# Target: example_KafkaConsumer_Simple
add_executable("example_KafkaConsumer_Simple" "example_KafkaConsumer_Simple.cc")
target_link_libraries("example_KafkaConsumer_Simple" modern-cpp-kafka-api)

# Target: example_KafkaConsumer_RebalanceEvents
add_executable("example_KafkaConsumer_RebalanceEvents" "example_KafkaConsumer_RebalanceEvents.cc")
target_link_libraries("example_KafkaConsumer_RebalanceEvents" modern-cpp-kafka-api)

# Target: example_KafkaConsumer_ManualOffsetCommit
add_executable("example_KafkaConsumer_ManualOffsetCommit" "example_KafkaConsumer_ManualOffsetCommit.cc")
target_link_libraries("example_KafkaConsumer_ManualOffsetCommit" modern-cpp-kafka-api)

# Target: example_KafkaClient_Callbacks
add_executable("example_KafkaClient_Callbacks" "example_KafkaClient_Callbacks.cc")
target_link_libraries("example_KafkaClient_Callbacks" modern-cpp-kafka-api)
