#pragma once

#include <kafka/Project.h>

#include <kafka/KafkaClient.h>
#include <kafka/KafkaProducer.h>
#include <kafka/Types.h>

#include <deque>
#include <mutex>
#include <vector>

namespace KAFKA_API { namespace clients {

class KafkaRecoverableProducer
{
public:
    explicit KafkaRecoverableProducer(const Properties& properties)
        : _properties(properties), _running(true)
    {
        _errorCb = [this](const Error& error) {
            if (error.isFatal()) _fatalError = std::make_unique<Error>(error);
        };

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
        std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->clientId();
    }

    /**
     * Get the client name (i.e. client type + id).
     */
    const std::string& name() const
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->name();
    }

    /**
     * Set the log callback for the kafka client (it's a per-client setting).
     */
    void setLogger(const Logger& logger)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _logger = logger;
        _producer->setLogger(*_logger);
    }

    /**
     * Set log level for the kafka client (the default value: 5).
     */
    void setLogLevel(int level)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _logLevel = level;
        _producer->setLogLevel(*_logLevel);
    }

    /**
     * Set callback to receive the periodic statistics info.
     * Note: 1) It only works while the "statistics.interval.ms" property is configured with a non-0 value.
     *       2) The callback would be triggered periodically, receiving the internal statistics info (with JSON format) emited from librdkafka.
     */
    void setStatsCallback(const KafkaClient::StatsCallback& cb)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _statsCb = cb;
        _producer->setStatsCallback(*_statsCb);
    }

    void setErrorCallback(const KafkaClient::ErrorCallback& cb)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _errorCb = [cb, this](const Error& error) {
            cb(error);

            if (error.isFatal()) _fatalError = std::make_unique<Error>(error);
        };
        _producer->setErrorCallback(*_errorCb);
    }

    /**
     * Return the properties which took effect.
     */
    const Properties& properties() const
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->properties();
    }

    /**
     * Fetch the effected property (including the property internally set by librdkafka).
     */
    Optional<std::string> getProperty(const std::string& name) const
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

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
        std::lock_guard<std::mutex> lock(_producerMutex);

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
        std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->flush(timeout);
    }

    /**
     * Purge messages currently handled by the KafkaProducer.
     */
    Error purge()
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        return _producer->purge();
    }

    /**
     * Close this producer. This method would wait up to timeout for the producer to complete the sending of all incomplete requests (before purging them).
     */
    void close(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

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
        std::lock_guard<std::mutex> lock(_producerMutex);

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
        std::lock_guard<std::mutex> lock(_producerMutex);

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
        std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->send(record, deliveryCb, error, option, action);
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     */
    void initTransactions(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->initTransactions(timeout);
    }

    /**
     * Should be called before the start of each new transaction.
     */
    void beginTransaction()
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->beginTransaction();
    }

    /**
     * Commit the ongoing transaction.
     */
    void commitTransaction(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->commitTransaction(timeout);
    }

    /**
     * Abort the ongoing transaction.
     */
    void abortTransaction(std::chrono::milliseconds timeout = InfiniteTimeout)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

        _producer->abortTransaction(timeout);
    }

    /**
     * Send a list of specified offsets to the consumer group coodinator, and also marks those offsets as part of the current transaction.
     */
    void sendOffsetsToTransaction(const TopicPartitionOffsets&           topicPartitionOffsets,
                                  const consumer::ConsumerGroupMetadata& groupMetadata,
                                  std::chrono::milliseconds              timeout)
    {
        std::lock_guard<std::mutex> lock(_producerMutex);

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

                std::lock_guard<std::mutex> lock(_producerMutex);

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
        auto producer = std::make_unique<KafkaProducer>(_properties, KafkaClient::EventsPollingOption::Manual);

        if (_logger)   producer->setLogger(*_logger);
        if (_logLevel) producer->setLogLevel(*_logLevel);
        if (_statsCb)  producer->setStatsCallback(*_statsCb);
        if (_errorCb)  producer->setErrorCallback(*_errorCb);

        return producer;
    }

    // Configurations for producer
    Properties                           _properties;
    Optional<Logger>                     _logger;
    Optional<int>                        _logLevel;
    Optional<KafkaClient::StatsCallback> _statsCb;
    Optional<KafkaClient::ErrorCallback> _errorCb;

    std::unique_ptr<Error> _fatalError;

    std::atomic<bool> _running;
    std::thread       _pollThread;

    mutable std::mutex             _producerMutex;
    std::unique_ptr<KafkaProducer> _producer;
};

} } // end of KAFKA_API::clients

