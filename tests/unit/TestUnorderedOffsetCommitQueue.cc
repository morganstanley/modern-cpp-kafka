#include "kafka/addons/UnorderedOffsetCommitQueue.h"

#include "gtest/gtest.h"

#include <algorithm>
#include <chrono>
#include <vector>

#if defined(WIN32)
#ifdef min
#undef min
#endif
#endif


namespace Kafka = KAFKA_API;

TEST(UnorderedOffsetCommitQueue, Functionality)
{
    Kafka::UnorderedOffsetCommitQueue queue;

    // Suppose consumer received some records with a sigle `poll`, and forwarded them to several handlers
    queue.waitOffset(1);
    queue.waitOffset(2);
    queue.waitOffset(3);
    queue.waitOffset(4);
    queue.waitOffset(5);
    queue.waitOffset(6);
    queue.waitOffset(7);
    queue.waitOffset(8);
    queue.waitOffset(9);

    // Suppose these handlers would ack these offsets occasionaly
    // And we'll check whether we could get the right offset to commit
    queue.ackOffset(3);
    EXPECT_FALSE(queue.popOffsetToCommit());
    EXPECT_FALSE(queue.lastPoppedOffset());

    queue.ackOffset(2);
    EXPECT_FALSE(queue.popOffsetToCommit());
    EXPECT_FALSE(queue.lastPoppedOffset());

    queue.ackOffset(5);
    EXPECT_FALSE(queue.popOffsetToCommit());
    EXPECT_FALSE(queue.lastPoppedOffset());

    queue.ackOffset(1);
    auto offset = queue.popOffsetToCommit();
    EXPECT_EQ(*offset, queue.lastPoppedOffset());
    EXPECT_EQ(3 + 1, *offset);

    // No new offset to commit
    offset = queue.popOffsetToCommit();
    EXPECT_FALSE(offset);

    queue.ackOffset(4);
    offset = queue.popOffsetToCommit();
    EXPECT_EQ(5 + 1, *offset);

    queue.ackOffset(7);
    offset = queue.popOffsetToCommit();
    EXPECT_FALSE(offset);

    queue.ackOffset(6);
    offset = queue.popOffsetToCommit();
    EXPECT_EQ(*offset, *queue.lastPoppedOffset());
    EXPECT_EQ(7 + 1, *offset);

    queue.ackOffset(8);
    offset = queue.popOffsetToCommit();
    EXPECT_EQ(8 + 1, *offset);

    queue.ackOffset(9);
    offset = queue.popOffsetToCommit();
    EXPECT_EQ(9 + 1, *offset);

    // No more records to commit
    offset = queue.popOffsetToCommit();
    EXPECT_FALSE(offset);
}

TEST(UnorderedOffsetCommitQueue, AbnormalCases)
{
    Kafka::UnorderedOffsetCommitQueue queue("some-topic", 2);

    queue.waitOffset(1);
    queue.waitOffset(2);
    // duplicated offset
    queue.waitOffset(2);
    // invalid offset
    queue.waitOffset(-1);
    queue.waitOffset(3);
    queue.waitOffset(4);
    queue.waitOffset(5);

    queue.ackOffset(3);
    auto offset = queue.popOffsetToCommit();
    EXPECT_FALSE(offset);

    queue.ackOffset(2);
    offset = queue.popOffsetToCommit();
    EXPECT_FALSE(offset);

    queue.ackOffset(1);
    offset = queue.popOffsetToCommit();
    EXPECT_EQ(3 + 1, *offset);

    // ack an offset even smaller than expected
    queue.ackOffset(2);
    offset = queue.popOffsetToCommit();
    EXPECT_FALSE(offset);

    // ack an offset even smaller than expected
    queue.ackOffset(6);
    offset = queue.popOffsetToCommit();
    EXPECT_FALSE(offset);

    queue.ackOffset(4);
    offset = queue.popOffsetToCommit();
    EXPECT_EQ(4 + 1, *offset);

    // Now only 1 offset left un-popped
    EXPECT_EQ(1, queue.size());
}


namespace {

auto checkTimeMsConsumedToSortOffsets(std::size_t testNum, std::size_t step)
{
    Kafka::UnorderedOffsetCommitQueue queue;

    std::vector<Kafka::Offset> waitSequence(testNum);
    for (std::size_t i = 0 ; i < testNum; ++i)
    {
        waitSequence[i] = i;
    }

    std::vector<Kafka::Offset> ackSequence = waitSequence;
    std::random_device rd;
    std::mt19937 g(rd());
    for (std::size_t iBegin = 0; iBegin < ackSequence.size(); iBegin += step)
    {
        std::size_t iEnd = std::min(iBegin + step, ackSequence.size());
        std::shuffle(ackSequence.begin() + iBegin, ackSequence.begin() + iEnd, g);
    }

    using namespace std::chrono;
    auto timestampBegin = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    std::size_t indexWait = 0;
    std::size_t indexAck = 0;
    while (indexAck < testNum)
    {
        for (std::size_t i = 0; i < step && indexWait < testNum; ++i)
        {
            queue.waitOffset(waitSequence[indexWait++]);
        }

        for (std::size_t i = 0; i < step && indexAck < testNum; ++i)
        {
            queue.ackOffset(ackSequence[indexAck++]);
        }
    }

    auto timestampEnd = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    // All offsets have been acked
    EXPECT_EQ(testNum, *queue.popOffsetToCommit());

    return (timestampEnd - timestampBegin);
}

} // end of namespace

TEST(UnorderedOffsetCommitQueue, CheckPerf)
{
    std::size_t testNum = 1000000;
    std::size_t step = 100;
    std::cout << "Took " << checkTimeMsConsumedToSortOffsets(testNum, step) << " ms to sort " << testNum << " offsets (with step:" << step << ")." << std::endl;

    testNum = 1000000;
    step = 1000;
    std::cout << "Took " << checkTimeMsConsumedToSortOffsets(testNum, step) << " ms to sort " << testNum << " offsets (with step:" << step << ")." << std::endl;

    testNum = 1000000;
    step = 10000;
    std::cout << "Took " << checkTimeMsConsumedToSortOffsets(testNum, step) << " ms to sort " << testNum << " offsets (with step:" << step << ")." << std::endl;

    testNum = 1000000;
    step = 100000;
    std::cout << "Took " << checkTimeMsConsumedToSortOffsets(testNum, step) << " ms to sort " << testNum << " offsets (with step:" << step << ")." << std::endl;
}

