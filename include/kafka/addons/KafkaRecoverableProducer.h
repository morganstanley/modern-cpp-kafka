#pragma once

#include <kafka/Project.h>

#include <kafka/KafkaProducer.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>


namespace KAFKA_API { namespace clients { namespace producer {

class KafkaRecoverableProducer
{
public:
    explicit KafkaRecoverableProducer(const Properties& properties)
        : _properties(properties), _running(true)
    {
        _properties.put(Config::ENABLE_MANUAL_EVENTS_POLL, "true");
        _properties.put(Config::ERROR_CB, [this](const Error& error) { if (error.isFatal()) _fatalError = std::make_unique<Error>(error); });

        _producer = createProducer();

        _pollThread = std::thread([this]() { keepPolling(); });
    }

    ~KafkaRecoverableProducer()
    {
        if (_running) close();
    }

    /**
     * Get the client id.
     */
    const std::string& clientId() const
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->clientId();
    }

    /**
     * Get the client name (i.e. client type + id).
     */
    const std::string& name() const
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->name();
    }

    /**
     * Set log level for the kafka client (the default value: 5).
     */
    void setLogLevel(int level)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _properties.put(Config::LOG_LEVEL, std::to_string(level));
        _producer->setLogLevel(level);
    }

    /**
     * Return the properties which took effect.
     */
    const Properties& properties() const
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->properties();
    }

    /**
     * Fetch the effected property (including the property internally set by librdkafka).
     */
    Optional<std::string> getProperty(const std::string& name) const
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->getProperty(name);
    }

    /**
     * Fetch matadata from a available broker.
     * Note: the Metadata response information may trigger a re-join if any subscribed topic has changed partition count or existence state.
     */
    Optional<BrokerMetadata> fetchBrokerMetadata(const std::string& topic,
                                                 std::chrono::milliseconds timeout = std::chrono::milliseconds(KafkaClient::DEFAULT_METADATA_TIMEOUT_MS),
                                                 bool disableErrorLogging = false)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->fetchBrokerMetadata(topic, timeout, disableErrorLogging);
    }

    /**
     * Invoking this method makes all buffered records immediately available to send, and blocks on the completion of the requests associated with these records.
     *
     * Possible error values:
     *   - RD_KAFKA_RESP_ERR__TIMED_OUT: The `timeout` was reached before all outstanding requests were completed.
     */
    Error flush(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->flush(timeout);
    }

    /**
     * Purge messages currently handled by the KafkaProducer.
     */
    Error purge()
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->purge();
    }

    /**
     * Close this producer. This method would wait up to timeout for the producer to complete the sending of all incomplete requests (before purging them).
     */
    void close(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _running = false;
        if (_pollThread.joinable()) _pollThread.join();

        _producer->close(timeout);
    }

    /**
     * Synchronously send a record to a topic.
     * Throws KafkaException with errors:
     *   Local errors,
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:     The topic doesn't exist
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: The partition doesn't exist
     *     - RD_KAFKA_RESP_ERR__INVALID_ARG:       Invalid topic(topic is null, or the length is too long (> 512)
     *     - RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:     No ack received within the time limit
     *   Broker errors,
     *     - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
     */
    producer::RecordMetadata syncSend(const producer::ProducerRecord& record)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->syncSend(record);
    }

    /**
     * Asynchronously send a record to a topic.
     *
     * Note:
     *   - If a callback is provided, it's guaranteed to be triggered (before closing the producer).
     *   - If any error occured, an exception would be thrown.
     *   - Make sure the memory block (for ProducerRecord's value) is valid until the delivery callback finishes; Otherwise, should be with option `KafkaProducer::SendOption::ToCopyRecordValue`.
     *
     * Possible errors:
     *   Local errors,
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:     The topic doesn't exist
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: The partition doesn't exist
     *     - RD_KAFKA_RESP_ERR__INVALID_ARG:       Invalid topic(topic is null, or the length is too long (> 512)
     *     - RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:     No ack received within the time limit
     *     - RD_KAFKA_RESP_ERR__QUEUE_FULL:        The message buffing queue is full
     *   Broker errors,
     *     - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
     */
    void send(const producer::ProducerRecord&       record,
              const producer::Callback&             deliveryCb,
              KafkaProducer::SendOption             option = KafkaProducer::SendOption::NoCopyRecordValue,
              KafkaProducer::ActionWhileQueueIsFull action = KafkaProducer::ActionWhileQueueIsFull::Block)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->send(record, deliveryCb, option, action);
    }

    /**
     * Asynchronously send a record to a topic.
     *
     * Note:
     *   - If a callback is provided, it's guaranteed to be triggered (before closing the producer).
     *   - The input reference parameter `error` will be set if an error occurred.
     *   - Make sure the memory block (for ProducerRecord's value) is valid until the delivery callback finishes; Otherwise, should be with option `KafkaProducer::SendOption::ToCopyRecordValue`.
     *
     * Possible errors:
     *   Local errors,
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:     The topic doesn't exist
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: The partition doesn't exist
     *     - RD_KAFKA_RESP_ERR__INVALID_ARG:       Invalid topic(topic is null, or the length is too long (> 512)
     *     - RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:     No ack received within the time limit
     *     - RD_KAFKA_RESP_ERR__QUEUE_FULL:        The message buffing queue is full
     *   Broker errors,
     *     - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
     */

    void send(const producer::ProducerRecord&       record,
              const producer::Callback&             deliveryCb,
              Error&                                error,
              KafkaProducer::SendOption             option = KafkaProducer::SendOption::NoCopyRecordValue,
              KafkaProducer::ActionWhileQueueIsFull action = KafkaProducer::ActionWhileQueueIsFull::Block)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->send(record, deliveryCb, error, option, action);
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     */
    void initTransactions(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->initTransactions(timeout);
    }

    /**
     * Should be called before the start of each new transaction.
     */
    void beginTransaction()
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->beginTransaction();
    }

    /**
     * Commit the ongoing transaction.
     */
    void commitTransaction(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->commitTransaction(timeout);
    }

    /**
     * Abort the ongoing transaction.
     */
    void abortTransaction(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->abortTransaction(timeout);
    }

    /**
     * Send a list of specified offsets to the consumer group coodinator, and also marks those offsets as part of the current transaction.
     */
    void sendOffsetsToTransaction(const TopicPartitionOffsets&           topicPartitionOffsets,
                                  const consumer::ConsumerGroupMetadata& groupMetadata,
                                  std::chrono::milliseconds              timeout)
    {
        const std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->sendOffsetsToTransaction(topicPartitionOffsets, groupMetadata, timeout);
    }

#ifdef KAFKA_API_ENABLE_UNIT_TEST_STUBS
    void mockFatalError()
    {
        _fatalError = std::make_unique<Error>(RD_KAFKA_RESP_ERR__FATAL, "fake fatal error", true);
    }
#endif

private:
    void keepPolling()
    {
        while (_running)
        {
            _producer->pollEvents(std::chrono::milliseconds(1));
            if (_fatalError)
            {
                const std::string errStr = _fatalError->toString();
                KAFKA_API_LOG(Log::Level::Notice, "met fatal error[%s], will re-initialize the internal producer", errStr.c_str());

                const std::lock_guard<std::mutex> lock(_producerMutex);

                if (!_running) return;

                _producer->purge();
                _producer->close();

                _fatalError.reset();

                _producer = createProducer();
            }
        }
    }

    std::unique_ptr<KafkaProducer> createProducer()
    {
        return std::make_unique<KafkaProducer>(_properties);
    }

    // Configurations for producer
    Properties             _properties;

    std::unique_ptr<Error> _fatalError;

    std::atomic<bool> _running;
    std::thread       _pollThread;

    mutable std::mutex             _producerMutex;
    std::unique_ptr<KafkaProducer> _producer;
};

} } } // end of KAFKA_API::clients::producer

