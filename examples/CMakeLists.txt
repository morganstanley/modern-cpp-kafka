project("kafka-examples")

# Target: kafka_sync_producer
add_executable("kafka_sync_producer" "kafka_sync_producer.cc")
target_link_libraries("kafka_sync_producer" modern-cpp-kafka-api ${SASL_LIBRARY})

# Target: kafka_async_producer_copy_payload
add_executable("kafka_async_producer_copy_payload" "kafka_async_producer_copy_payload.cc")
target_link_libraries("kafka_async_producer_copy_payload" modern-cpp-kafka-api)
target_link_libraries("kafka_async_producer_copy_payload" "pthread" ${SASL_LIBRARY})

# Target: kafka_async_producer_not_copy_payload
add_executable("kafka_async_producer_not_copy_payload" "kafka_async_producer_not_copy_payload.cc")
target_link_libraries("kafka_async_producer_not_copy_payload" modern-cpp-kafka-api)
target_link_libraries("kafka_async_producer_not_copy_payload" "pthread" ${SASL_LIBRARY})

# Target: kafka_auto_commit_consumer
add_executable("kafka_auto_commit_consumer" "kafka_auto_commit_consumer.cc")
target_link_libraries("kafka_auto_commit_consumer" modern-cpp-kafka-api ${SASL_LIBRARY})

# Target: kafka_manual_commit_consumer
add_executable("kafka_manual_commit_consumer" "kafka_manual_commit_consumer.cc")
target_link_libraries("kafka_manual_commit_consumer" modern-cpp-kafka-api)
target_link_libraries("kafka_manual_commit_consumer" "pthread" ${SASL_LIBRARY})
