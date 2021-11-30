#include <kafka/addons/KafkaMetrics.h>

#include "gtest/gtest.h"

#include <chrono>
#include <iostream>
#include <vector>

namespace {

const std::string consumerMetricsSample = "{"
    "\"name\": \"3b0dd279-b3961b63#consumer-2\","
    "\"client_id\": \"3b0dd279-b3961b63\","
    "\"type\": \"consumer\","
    "\"ts\":2055955903075,"
    "\"time\":1637570619,"
    "\"age\":5000965,"
    "\"replyq\":0,"
    "\"msg_cnt\":0,"
    "\"msg_size\":0,"
    "\"msg_max\":0,"
    "\"msg_size_max\":0,"
    "\"simple_cnt\":0,"
    "\"metadata_cache_cnt\":1,"
    "\"brokers\": {"
        "\"127.0.0.1:29003/2\": {"
            "\"name\":\"127.0.0.1:29003/2\","
            "\"nodeid\":2,"
            "\"nodename\":\"127.0.0.1:29003\","
            "\"source\":\"configured\","
            "\"state\":\"UP\","
            "\"stateage\":4978600,"
            "\"outbuf_cnt\":0,"
            "\"outbuf_msg_cnt\":0,"
            "\"waitresp_cnt\":0,"
            "\"waitresp_msg_cnt\":0,"
            "\"tx\":5,"
            "\"txbytes\":243,"
            "\"txerrs\":0,"
            "\"txretries\":0,"
            "\"txidle\":4976585,"
            "\"req_timeouts\":0,"
            "\"rx\":5,"
            "\"rxbytes\":718,"
            "\"rxerrs\":0,"
            "\"rxcorriderrs\":0,"
            "\"rxpartial\":0,"
            "\"rxidle\":4976007,"
            "\"zbuf_grow\":0,"
            "\"buf_grow\":0,"
            "\"wakeups\":15,"
            "\"connects\":1,"
            "\"disconnects\":0,"
            "\"int_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 },"
            "\"outbuf_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 },"
            "\"rtt\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 13424, \"cnt\":0 },"
            "\"throttle\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 17520, \"cnt\":0 },"
            "\"req\": {"
                "\"Fetch\": 0,"
                "\"ListOffsets\": 0,"
                "\"Metadata\": 2,"
                "\"OffsetCommit\": 0,"
                "\"OffsetFetch\": 0,"
                "\"FindCoordinator\": 2,"
                "\"JoinGroup\": 0,"
                "\"Heartbeat\": 0,"
                "\"LeaveGroup\": 0,"
                "\"SyncGroup\": 0,"
                "\"SaslHandshake\": 0,"
                "\"ApiVersion\": 1,"
                "\"SaslAuthenticate\": 0,"
                "\"OffsetDeleteRequest\": 0,"
                "\"DescribeClientQuotasRequest\": 0,"
                "\"AlterClientQuotasRequest\": 0,"
                "\"DescribeUserScramCredentialsRequest\": 0 },"
            "\"toppars\":{ } } ,"
        "\"127.0.0.1:29002/1\": { \"name\":\"127.0.0.1:29002/1\", \"nodeid\":1, \"nodename\":\"127.0.0.1:29002\", \"source\":\"configured\", \"state\":\"INIT\", \"stateage\":5000678, \"outbuf_cnt\":0, \"outbuf_msg_cnt\":0, \"waitresp_cnt\":0, \"waitresp_msg_cnt\":0, \"tx\":0, \"txbytes\":0, \"txerrs\":0, \"txretries\":0, \"txidle\":-1, \"req_timeouts\":0, \"rx\":0, \"rxbytes\":0, \"rxerrs\":0, \"rxcorriderrs\":0, \"rxpartial\":0, \"rxidle\":-1, \"zbuf_grow\":0, \"buf_grow\":0, \"wakeups\":0, \"connects\":0, \"disconnects\":0, \"int_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 }, \"outbuf_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 }, \"rtt\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 13424, \"cnt\":0 }, \"throttle\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 17520, \"cnt\":0 }, \"req\": { \"Fetch\": 0, \"ListOffsets\": 0, \"Metadata\": 0, \"OffsetCommit\": 0, \"OffsetFetch\": 0, \"FindCoordinator\": 0, \"JoinGroup\": 0, \"Heartbeat\": 0, \"LeaveGroup\": 0, \"SyncGroup\": 0, \"SaslHandshake\": 0, \"ApiVersion\": 0, \"SaslAuthenticate\": 0, \"OffsetDeleteRequest\": 0, \"DescribeClientQuotasRequest\": 0, \"AlterClientQuotasRequest\": 0, \"DescribeUserScramCredentialsRequest\": 0 }, \"toppars\":{ } } ,"
        "\"127.0.0.1:29001/0\": { \"name\":\"127.0.0.1:29001/0\", \"nodeid\":0, \"nodename\":\"127.0.0.1:29001\", \"source\":\"configured\", \"state\":\"UP\", \"stateage\":1822536, \"outbuf_cnt\":0, \"outbuf_msg_cnt\":0, \"waitresp_cnt\":1, \"waitresp_msg_cnt\":0, \"tx\":8, \"txbytes\":743, \"txerrs\":0, \"txretries\":0, \"txidle\":312285, \"req_timeouts\":0, \"rx\":7, \"rxbytes\":810, \"rxerrs\":0, \"rxcorriderrs\":0, \"rxpartial\":0, \"rxidle\":312319, \"zbuf_grow\":0, \"buf_grow\":0, \"wakeups\":17, \"connects\":1, \"disconnects\":0, \"int_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 }, \"outbuf_latency\": { \"min\":22, \"max\":37, \"avg\":27, \"sum\":110, \"stddev\": 5, \"p50\": 23, \"p75\": 28, \"p90\": 37, \"p95\": 37, \"p99\": 37, \"p99_99\": 37, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":4 }, \"rtt\": { \"min\":111290, \"max\":507504, \"avg\":307785, \"sum\":1231143, \"stddev\": 196236, \"p50\": 111615, \"p75\": 501759, \"p90\": 507903, \"p95\": 507903, \"p99\": 507903, \"p99_99\": 507903, \"outofrange\": 0, \"hdrsize\": 13424, \"cnt\":4 }, \"throttle\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 17520, \"cnt\":0 }, \"req\": { \"Fetch\": 4, \"ListOffsets\": 3, \"Metadata\": 0, \"OffsetCommit\": 0, \"OffsetFetch\": 0, \"FindCoordinator\": 0, \"JoinGroup\": 0, \"Heartbeat\": 0, \"LeaveGroup\": 0, \"SyncGroup\": 0, \"SaslHandshake\": 0, \"ApiVersion\": 1, \"SaslAuthenticate\": 0, \"OffsetDeleteRequest\": 0, \"DescribeClientQuotasRequest\": 0, \"AlterClientQuotasRequest\": 0, \"DescribeUserScramCredentialsRequest\": 0 },"
            "\"toppars\":{"
                "\"0d08e094-cea41296-0\": { \"topic\":\"0d08e094-cea41296\", \"partition\":0} } } ,"
        "\"GroupCoordinator\": {"
            "\"name\":\"GroupCoordinator\","
            "\"nodeid\":0,"
            "\"nodename\":\"127.0.0.1:29001\","
            "\"source\":\"logical\","
            "\"state\":\"UP\","
            "\"stateage\":4976637,"
            "\"outbuf_cnt\":0,"
            "\"outbuf_msg_cnt\":0,"
            "\"waitresp_cnt\":0,"
            "\"waitresp_msg_cnt\":0,"
            "\"tx\":8,"
            "\"txbytes\":935,"
            "\"txerrs\":0,"
            "\"txretries\":0,"
            "\"txidle\":1925182,"
            "\"req_timeouts\":0,"
            "\"rx\":8,"
            "\"rxbytes\":1084,"
            "\"rxerrs\":0,"
            "\"rxcorriderrs\":0,"
            "\"rxpartial\":0,"
            "\"rxidle\":1923901,"
            "\"zbuf_grow\":0,"
            "\"buf_grow\":0,"
            "\"wakeups\":22,"
            "\"connects\":1,"
            "\"disconnects\":0,"
            "\"int_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 },"
            "\"outbuf_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 },"
            "\"rtt\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 16496, \"cnt\":0 },"
            "\"throttle\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 17520, \"cnt\":0 },"
            "\"req\": {"
                "\"Fetch\": 0,"
                "\"ListOffsets\": 0,"
                "\"Metadata\": 2,"
                "\"OffsetCommit\": 0,"
                "\"OffsetFetch\": 1,"
                "\"FindCoordinator\": 0,"
                "\"JoinGroup\": 2,"
                "\"Heartbeat\": 1,"
                "\"LeaveGroup\": 0,"
                "\"SyncGroup\": 1,"
                "\"SaslHandshake\": 0,"
                "\"ApiVersion\": 1,"
                "\"SaslAuthenticate\": 0,"
                "\"OffsetDeleteRequest\": 0,"
                "\"DescribeClientQuotasRequest\": 0,"
                "\"AlterClientQuotasRequest\": 0,"
                "\"DescribeUserScramCredentialsRequest\": 0 },"
            "\"toppars\":{ } } },"
    "\"topics\":{"
        "\"0d08e094-cea41296\": {"
            "\"topic\":\"0d08e094-cea41296\","
            "\"age\":1925,"
            "\"metadata_age\":1971,"
            "\"batchsize\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 14448, \"cnt\":0 },"
            "\"batchcnt\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 8304, \"cnt\":0 },"
            "\"partitions\":{"
                "\"0\": { "
                    "\"partition\":0,"
                    "\"broker\":0,"
                    "\"leader\":0,"
                    "\"desired\":true,"
                    "\"unknown\":false,"
                    "\"msgq_cnt\":0,"
                    "\"msgq_bytes\":0,"
                    "\"xmit_msgq_cnt\":0,"
                    "\"xmit_msgq_bytes\":0,"
                    "\"fetchq_cnt\":0,"
                    "\"fetchq_size\":0,"
                    "\"fetch_state\":\"active\","
                    "\"query_offset\":-1,"
                    "\"next_offset\":0,"
                    "\"app_offset\":-1001,"
                    "\"stored_offset\":-1001,"
                    "\"commited_offset\":-1001,"
                    "\"committed_offset\":-1001,"
                    "\"eof_offset\":0,"
                    "\"lo_offset\":0,"
                    "\"hi_offset\":0,"
                    "\"ls_offset\":0,"
                    "\"consumer_lag\":123,"
                    "\"consumer_lag_stored\":-1,"
                    "\"txmsgs\":0,"
                    "\"txbytes\":0,"
                    "\"rxmsgs\":0,"
                    "\"rxbytes\":0,"
                    "\"msgs\": 0,"
                    "\"rx_ver_drops\": 0,"
                    "\"msgs_inflight\": 0,"
                    "\"next_ack_seq\": 0,"
                    "\"next_err_seq\": 0,"
                    "\"acked_msgid\": 0} ,"
                "\"-1\": {"
                    "\"partition\":-1, \"broker\":-1, \"leader\":-1, \"desired\":false, \"unknown\":false, \"msgq_cnt\":0, \"msgq_bytes\":0, \"xmit_msgq_cnt\":0, \"xmit_msgq_bytes\":0, \"fetchq_cnt\":0, \"fetchq_size\":0, \"fetch_state\":\"none\", \"query_offset\":-1001, \"next_offset\":0, \"app_offset\":-1001, \"stored_offset\":-1001, \"commited_offset\":-1001, \"committed_offset\":-1001, \"eof_offset\":-1001, \"lo_offset\":-1001, \"hi_offset\":-1001, \"ls_offset\":-1001, \"consumer_lag\":-1, \"consumer_lag_stored\":-1, \"txmsgs\":0, \"txbytes\":0, \"rxmsgs\":0, \"rxbytes\":0, \"msgs\": 0, \"rx_ver_drops\": 0, \"msgs_inflight\": 0, \"next_ack_seq\": 0, \"next_err_seq\": 0, \"acked_msgid\": 0} } } } ,"
    "\"cgrp\": {"
        "\"state\": \"up\","
        "\"stateage\": 4976,"
        "\"join_state\": \"steady\","
        "\"rebalance_age\": 1925,"
        "\"rebalance_cnt\": 1,"
        "\"rebalance_reason\": \"Metadata for subscribed topic(s) has changed\","
        "\"assignment_size\": 1 },"
    "\"tx\":21,"
    "\"tx_bytes\":1921,"
    "\"rx\":20,"
    "\"rx_bytes\":2612,"
    "\"txmsgs\":0,"
    "\"txmsg_bytes\":0,"
    "\"rxmsgs\":0,"
    "\"rxmsg_bytes\":0}";

const std::string producerMetricsSample = "{"
    "\"name\": \"d9b17fbf-3ab8fe20#producer-2\","
    "\"client_id\": \"d9b17fbf-3ab8fe20\","
    "\"type\": \"producer\","
    "\"ts\":167797190908,"
    "\"time\":1637740702,"
    "\"age\":10155,"
    "\"replyq\":0,"
    "\"msg_cnt\":3000,"
    "\"msg_size\":18000,"
    "\"msg_max\":100000,"
    "\"msg_size_max\":1073741824,"
    "\"simple_cnt\":0,"
    "\"metadata_cache_cnt\":1,"
    "\"brokers\":{"
        "\"127.0.0.1:29003/2\": {"
            "\"name\":\"127.0.0.1:29003/2\","
            "\"nodeid\":2,"
            "\"nodename\":\"127.0.0.1:29003\","
            "\"source\":\"configured\","
            "\"state\":\"UP\","
            "\"stateage\":5083,"
            "\"outbuf_cnt\":0,"
            "\"outbuf_msg_cnt\":1,"
            "\"waitresp_cnt\":1,"
            "\"waitresp_msg_cnt\":1588,"
            "\"tx\":2,"
            "\"txbytes\":28705,"
            "\"txerrs\":0,"
            "\"txretries\":0,"
            "\"txidle\":4229,"
            "\"req_timeouts\":0,"
            "\"rx\":1,"
            "\"rxbytes\":366,"
            "\"rxerrs\":0,"
            "\"rxcorriderrs\":0,"
            "\"rxpartial\":0,"
            "\"rxidle\":5103,"
            "\"zbuf_grow\":0,"
            "\"buf_grow\":0,"
            "\"wakeups\":493,"
            "\"connects\":1,"
            "\"disconnects\":0,"
            "\"int_latency\": { \"min\":10, \"max\":5009, \"avg\":2905, \"sum\":4613433, \"stddev\": 1573, \"p50\": 3455, \"p75\": 4255, \"p90\": 4703, \"p95\": 4863, \"p99\": 4991, \"p99_99\": 5023, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":1588 },"
            "\"outbuf_latency\": { \"min\":22, \"max\":25, \"avg\":23, \"sum\":47, \"stddev\": 1, \"p50\": 22, \"p75\": 25, \"p90\": 25, \"p95\": 25, \"p99\": 25, \"p99_99\": 25, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":2 },"
            "\"rtt\": { \"min\":1366, \"max\":1366, \"avg\":1366, \"sum\":1366, \"stddev\": 0, \"p50\": 1367, \"p75\": 1367, \"p90\": 1367, \"p95\": 1367, \"p99\": 1367, \"p99_99\": 1367, \"outofrange\": 0, \"hdrsize\": 13424, \"cnt\":1 },"
            "\"throttle\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 17520, \"cnt\":0 },"
            "\"req\": { "
                "\"Produce\": 1,"
                "\"ListOffsets\": 0,"
                "\"Metadata\": 0,"
                "\"FindCoordinator\": 0,"
                "\"SaslHandshake\": 0,"
                "\"ApiVersion\": 1,"
                "\"InitProducerId\": 0,"
                "\"AddPartitionsToTxn\": 0,"
                "\"AddOffsetsToTxn\": 0,"
                "\"EndTxn\": 0,"
                "\"TxnOffsetCommit\": 0,"
                "\"SaslAuthenticate\": 0,"
                "\"OffsetDeleteRequest\": 0,"
                "\"DescribeClientQuotasRequest\": 0,"
                "\"AlterClientQuotasRequest\": 0,"
                "\"DescribeUserScramCredentialsRequest\": 0 },"
            "\"toppars\":{"
                "\"a3d4fc1f-1ad3c53a-0\": {"
                    "\"topic\":\"a3d4fc1f-1ad3c53a\","
                    "\"partition\":0} ,"
                "\"a3d4fc1f-1ad3c53a-3\": {"
                    "\"topic\":\"a3d4fc1f-1ad3c53a\","
                    "\"partition\":3} } } ,"
        "\"127.0.0.1:29002/1\": { \"name\":\"127.0.0.1:29002/1\", \"nodeid\":1, \"nodename\":\"127.0.0.1:29002\", \"source\":\"configured\", \"state\":\"INIT\", \"stateage\":9938, \"outbuf_cnt\":0, \"outbuf_msg_cnt\":2, \"waitresp_cnt\":0, \"waitresp_msg_cnt\":0, \"tx\":0, \"txbytes\":0, \"txerrs\":0, \"txretries\":0, \"txidle\":-1, \"req_timeouts\":0, \"rx\":0, \"rxbytes\":0, \"rxerrs\":0, \"rxcorriderrs\":0, \"rxpartial\":0, \"rxidle\":-1, \"zbuf_grow\":0, \"buf_grow\":0, \"wakeups\":0, \"connects\":0, \"disconnects\":0, \"int_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 }, \"outbuf_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 }, \"rtt\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 13424, \"cnt\":0 }, \"throttle\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 17520, \"cnt\":0 }, \"req\": { \"Produce\": 0, \"ListOffsets\": 0, \"Metadata\": 0, \"FindCoordinator\": 0, \"SaslHandshake\": 0, \"ApiVersion\": 0, \"InitProducerId\": 0, \"AddPartitionsToTxn\": 0, \"AddOffsetsToTxn\": 0, \"EndTxn\": 0, \"TxnOffsetCommit\": 0, \"SaslAuthenticate\": 0, \"OffsetDeleteRequest\": 0, \"DescribeClientQuotasRequest\": 0, \"AlterClientQuotasRequest\": 0, \"DescribeUserScramCredentialsRequest\": 0 }, \"toppars\":{ \"a3d4fc1f-1ad3c53a-1\": { \"topic\":\"a3d4fc1f-1ad3c53a\", \"partition\":1} , \"a3d4fc1f-1ad3c53a-4\": { \"topic\":\"a3d4fc1f-1ad3c53a\", \"partition\":4} } } ,"
        "\"127.0.0.1:29001/0\": { \"name\":\"127.0.0.1:29001/0\", \"nodeid\":0, \"nodename\":\"127.0.0.1:29001\", \"source\":\"configured\", \"state\":\"UP\", \"stateage\":6840, \"outbuf_cnt\":0, \"outbuf_msg_cnt\":3, \"waitresp_cnt\":0, \"waitresp_msg_cnt\":0, \"tx\":2, \"txbytes\":105, \"txerrs\":0, \"txretries\":0, \"txidle\":7845, \"req_timeouts\":0, \"rx\":2, \"rxbytes\":713, \"rxerrs\":0, \"rxcorriderrs\":0, \"rxpartial\":0, \"rxidle\":6953, \"zbuf_grow\":0, \"buf_grow\":0, \"wakeups\":7, \"connects\":1, \"disconnects\":0, \"int_latency\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":0 }, \"outbuf_latency\": { \"min\":24, \"max\":27, \"avg\":25, \"sum\":51, \"stddev\": 1, \"p50\": 24, \"p75\": 27, \"p90\": 27, \"p95\": 27, \"p99\": 27, \"p99_99\": 27, \"outofrange\": 0, \"hdrsize\": 11376, \"cnt\":2 }, \"rtt\": { \"min\":892, \"max\":1661, \"avg\":1276, \"sum\":2553, \"stddev\": 383, \"p50\": 895, \"p75\": 1663, \"p90\": 1663, \"p95\": 1663, \"p99\": 1663, \"p99_99\": 1663, \"outofrange\": 0, \"hdrsize\": 13424, \"cnt\":2 }, \"throttle\": { \"min\":0, \"max\":0, \"avg\":0, \"sum\":0, \"stddev\": 0, \"p50\": 0, \"p75\": 0, \"p90\": 0, \"p95\": 0, \"p99\": 0, \"p99_99\": 0, \"outofrange\": 0, \"hdrsize\": 17520, \"cnt\":0 }, \"req\": { \"Produce\": 0, \"ListOffsets\": 0, \"Metadata\": 1, \"FindCoordinator\": 0, \"SaslHandshake\": 0, \"ApiVersion\": 1, \"InitProducerId\": 0, \"AddPartitionsToTxn\": 0, \"AddOffsetsToTxn\": 0, \"EndTxn\": 0, \"TxnOffsetCommit\": 0, \"SaslAuthenticate\": 0, \"OffsetDeleteRequest\": 0, \"DescribeClientQuotasRequest\": 0, \"AlterClientQuotasRequest\": 0, \"DescribeUserScramCredentialsRequest\": 0 }, \"toppars\":{ \"a3d4fc1f-1ad3c53a-2\": { \"topic\":\"a3d4fc1f-1ad3c53a\", \"partition\":2} } } },"
    "\"topics\":{"
        "\"a3d4fc1f-1ad3c53a\": {"
            "\"topic\":\"a3d4fc1f-1ad3c53a\","
            "\"age\":9,"
            "\"metadata_age\":6,"
            "\"batchsize\": { \"min\":28581, \"max\":28581, \"avg\":28581, \"sum\":28581, \"stddev\": 0, \"p50\": 28671, \"p75\": 28671, \"p90\": 28671, \"p95\": 28671, \"p99\": 28671, \"p99_99\": 28671, \"outofrange\": 0, \"hdrsize\": 14448, \"cnt\":1 },"
            "\"batchcnt\": { \"min\":1588, \"max\":1588, \"avg\":1588, \"sum\":1588, \"stddev\": 0, \"p50\": 1591, \"p75\": 1591, \"p90\": 1591, \"p95\": 1591, \"p99\": 1591, \"p99_99\": 1591, \"outofrange\": 0, \"hdrsize\": 8304, \"cnt\":1 },"
            "\"partitions\":{"
                "\"0\": {"
                    "\"partition\":0,"
                    "\"broker\":2,"
                    "\"leader\":2,"
                    "\"desired\":false,"
                    "\"unknown\":false,"
                    "\"msgq_cnt\":0,"
                    "\"msgq_bytes\":0,"
                    "\"xmit_msgq_cnt\":0,"
                    "\"xmit_msgq_bytes\":0,"
                    "\"fetchq_cnt\":0,"
                    "\"fetchq_size\":0,"
                    "\"fetch_state\":\"none\","
                    "\"query_offset\":-1001,"
                    "\"next_offset\":0,"
                    "\"app_offset\":-1001,"
                    "\"stored_offset\":-1001,"
                    "\"commited_offset\":-1001,"
                    "\"committed_offset\":-1001,"
                    "\"eof_offset\":-1001,"
                    "\"lo_offset\":-1001,"
                    "\"hi_offset\":-1001,"
                    "\"ls_offset\":-1001,"
                    "\"consumer_lag\":-1,"
                    "\"consumer_lag_stored\":-1,"
                    "\"txmsgs\":1588,"
                    "\"txbytes\":15880,"
                    "\"rxmsgs\":0,"
                    "\"rxbytes\":0,"
                    "\"msgs\": 3000,"
                    "\"rx_ver_drops\": 0,"
                    "\"msgs_inflight\": 1588,"
                    "\"next_ack_seq\": 0,"
                    "\"next_err_seq\": 0,"
                    "\"acked_msgid\": 0} ,"
                "\"1\": { \"partition\":1, \"broker\":1, \"leader\":1, \"desired\":false, \"unknown\":false, \"msgq_cnt\":0, \"msgq_bytes\":0, \"xmit_msgq_cnt\":0, \"xmit_msgq_bytes\":0, \"fetchq_cnt\":0, \"fetchq_size\":0, \"fetch_state\":\"none\", \"query_offset\":-1001, \"next_offset\":0, \"app_offset\":-1001, \"stored_offset\":-1001, \"commited_offset\":-1001, \"committed_offset\":-1001, \"eof_offset\":-1001, \"lo_offset\":-1001, \"hi_offset\":-1001, \"ls_offset\":-1001, \"consumer_lag\":-1, \"consumer_lag_stored\":-1, \"txmsgs\":0, \"txbytes\":0, \"rxmsgs\":0, \"rxbytes\":0, \"msgs\": 0, \"rx_ver_drops\": 0, \"msgs_inflight\": 0, \"next_ack_seq\": 0, \"next_err_seq\": 0, \"acked_msgid\": 0} ,"
                "\"2\": { \"partition\":2, \"broker\":0, \"leader\":0, \"desired\":false, \"unknown\":false, \"msgq_cnt\":0, \"msgq_bytes\":0, \"xmit_msgq_cnt\":0, \"xmit_msgq_bytes\":0, \"fetchq_cnt\":0, \"fetchq_size\":0, \"fetch_state\":\"none\", \"query_offset\":-1001, \"next_offset\":0, \"app_offset\":-1001, \"stored_offset\":-1001, \"commited_offset\":-1001, \"committed_offset\":-1001, \"eof_offset\":-1001, \"lo_offset\":-1001, \"hi_offset\":-1001, \"ls_offset\":-1001, \"consumer_lag\":-1, \"consumer_lag_stored\":-1, \"txmsgs\":0, \"txbytes\":0, \"rxmsgs\":0, \"rxbytes\":0, \"msgs\": 0, \"rx_ver_drops\": 0, \"msgs_inflight\": 0, \"next_ack_seq\": 0, \"next_err_seq\": 0, \"acked_msgid\": 0} ,"
                "\"3\": { \"partition\":3, \"broker\":2, \"leader\":2, \"desired\":false, \"unknown\":false, \"msgq_cnt\":0, \"msgq_bytes\":0, \"xmit_msgq_cnt\":0, \"xmit_msgq_bytes\":0, \"fetchq_cnt\":0, \"fetchq_size\":0, \"fetch_state\":\"none\", \"query_offset\":-1001, \"next_offset\":0, \"app_offset\":-1001, \"stored_offset\":-1001, \"commited_offset\":-1001, \"committed_offset\":-1001, \"eof_offset\":-1001, \"lo_offset\":-1001, \"hi_offset\":-1001, \"ls_offset\":-1001, \"consumer_lag\":-1, \"consumer_lag_stored\":-1, \"txmsgs\":0, \"txbytes\":0, \"rxmsgs\":0, \"rxbytes\":0, \"msgs\": 0, \"rx_ver_drops\": 0, \"msgs_inflight\": 0, \"next_ack_seq\": 0, \"next_err_seq\": 0, \"acked_msgid\": 0} ,"
                "\"4\": { \"partition\":4, \"broker\":1, \"leader\":1, \"desired\":false, \"unknown\":false, \"msgq_cnt\":0, \"msgq_bytes\":0, \"xmit_msgq_cnt\":0, \"xmit_msgq_bytes\":0, \"fetchq_cnt\":0, \"fetchq_size\":0, \"fetch_state\":\"none\", \"query_offset\":-1001, \"next_offset\":0, \"app_offset\":-1001, \"stored_offset\":-1001, \"commited_offset\":-1001, \"committed_offset\":-1001, \"eof_offset\":-1001, \"lo_offset\":-1001, \"hi_offset\":-1001, \"ls_offset\":-1001, \"consumer_lag\":-1, \"consumer_lag_stored\":-1, \"txmsgs\":0, \"txbytes\":0, \"rxmsgs\":0, \"rxbytes\":0, \"msgs\": 0, \"rx_ver_drops\": 0, \"msgs_inflight\": 0, \"next_ack_seq\": 0, \"next_err_seq\": 0, \"acked_msgid\": 0} ,"
                "\"-1\": { \"partition\":-1, \"broker\":-1, \"leader\":-1, \"desired\":false, \"unknown\":false, \"msgq_cnt\":0, \"msgq_bytes\":0, \"xmit_msgq_cnt\":0, \"xmit_msgq_bytes\":0, \"fetchq_cnt\":0, \"fetchq_size\":0, \"fetch_state\":\"none\", \"query_offset\":-1001, \"next_offset\":0, \"app_offset\":-1001, \"stored_offset\":-1001, \"commited_offset\":-1001, \"committed_offset\":-1001, \"eof_offset\":-1001, \"lo_offset\":-1001, \"hi_offset\":-1001, \"ls_offset\":-1001, \"consumer_lag\":-1, \"consumer_lag_stored\":-1, \"txmsgs\":0, \"txbytes\":0, \"rxmsgs\":0, \"rxbytes\":0, \"msgs\": 1000, \"rx_ver_drops\": 0, \"msgs_inflight\": 0, \"next_ack_seq\": 0, \"next_err_seq\": 0, \"acked_msgid\": 0} } } } ,"
    "\"tx\":4,"
    "\"tx_bytes\":28810,"
    "\"rx\":3,"
    "\"rx_bytes\":1079,"
    "\"txmsgs\":1588,"
    "\"txmsg_bytes\":15880,"
    "\"rxmsgs\":0,"
    "\"rxmsg_bytes\":0}";

} // end of namespace


TEST(KafkaMetrics, FailureCases)
{
    // Try invalid format for metrics
    try
    {
        kafka::KafkaMetrics invalidMetrics("{invalid: 3}");
        EXPECT_FALSE(true);
    }
    catch (const std::runtime_error& e) {}
    catch (...) { EXPECT_FALSE(true); }

    kafka::KafkaMetrics metrics(consumerMetricsSample);

    // Try invalid inputs (begin with "*")
    try
    {
        metrics.getInt({"*", "127.0.0.1:29003/2", "stateage"});
        EXPECT_FALSE(true);
    }
    catch (const std::invalid_argument& e) {}
    catch (...) { EXPECT_FALSE(true); }

    // Try invalid inputs (end with "*")
    try
    {
        metrics.getInt({"brokers", "127.0.0.1:29003/2", "*"});
        EXPECT_FALSE(true);
    }
    catch (const std::invalid_argument& e) {}
    catch (...) { EXPECT_FALSE(true); }

    // Try invalid inputs (no keys)
    try
    {
        metrics.getInt({});
        EXPECT_FALSE(true);
    }
    catch (const std::invalid_argument& e) {}
    catch (...) { EXPECT_FALSE(true); }

    // Try non-exist keys
    {
        auto results = metrics.getInt({"tx_nonexist"});
        EXPECT_TRUE(results.empty());
    }
}

TEST(KafkaMetrics, ParseConsumerMetrics)
{
    kafka::KafkaMetrics metrics(consumerMetricsSample);

    {
        auto results = metrics.getString({"type"});
        ASSERT_EQ(1, results.size());
        EXPECT_TRUE(results[0].first.empty());
        EXPECT_EQ("consumer", results[0].second);
    }

    {
        const std::vector<std::string> keys = {"brokers", "*", "state"};
        auto results = metrics.getString(keys);

        ASSERT_EQ(4, results.size());
        EXPECT_EQ(kafka::KafkaMetrics::KeysType{"127.0.0.1:29003/2"}, results[0].first);
        EXPECT_EQ("UP", results[0].second);
        EXPECT_EQ(kafka::KafkaMetrics::KeysType{"127.0.0.1:29002/1"}, results[1].first);
        EXPECT_EQ("INIT", results[1].second);
        EXPECT_EQ(kafka::KafkaMetrics::KeysType{"127.0.0.1:29001/0"}, results[2].first);
        EXPECT_EQ("UP", results[2].second);
        EXPECT_EQ(kafka::KafkaMetrics::KeysType{"GroupCoordinator"}, results[3].first);
        EXPECT_EQ("UP", results[3].second);

        std::cout << "Result for [" << kafka::KafkaMetrics::toString(keys) << "] ==> " << kafka::KafkaMetrics::toString(results) << std::endl;
    }

    // stateage: Time since last broker state change (microseconds)
    {
        const std::vector<std::string> keys = {"brokers", "*", "stateage"};
        auto results = metrics.getInt(keys);

        ASSERT_EQ(4, results.size());

        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29003/2"}, results[0].first);
        EXPECT_EQ(4978600, results[0].second);
        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29002/1"}, results[1].first);
        EXPECT_EQ(5000678, results[1].second);
        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29001/0"}, results[2].first);
        EXPECT_EQ(1822536, results[2].second);
        EXPECT_EQ(std::vector<std::string>{"GroupCoordinator"},  results[3].first);
        EXPECT_EQ(4976637, results[3].second);

        std::cout << "Result for [" << kafka::KafkaMetrics::toString(keys) << "] ==> " << kafka::KafkaMetrics::toString(results) << std::endl;
    }

    // consumer_lag: Difference between (hi_offset or ls_offset) and committed_offset).
    {
        const std::vector<std::string> keys = {"topics", "0d08e094-cea41296", "partitions", "0", "consumer_lag"};
        auto results = metrics.getInt(keys);

        ASSERT_EQ(1, results.size());
        EXPECT_EQ((std::vector<std::string>{}), results[0].first);
        EXPECT_EQ(123, results[0].second);

        std::cout << "Result for [" << kafka::KafkaMetrics::toString(keys) << "] ==> " << kafka::KafkaMetrics::toString(results) << std::endl;
    }

    {
        const std::vector<std::string> keys = {"topics", "*", "partitions", "*", "consumer_lag"};
        auto results = metrics.getInt(keys);

        ASSERT_EQ(2, results.size());
        EXPECT_EQ((std::vector<std::string>{"0d08e094-cea41296", "0"}), results[0].first);
        EXPECT_EQ(123, results[0].second);
        EXPECT_EQ((std::vector<std::string>{"0d08e094-cea41296", "-1"}), results[1].first);
        EXPECT_EQ(-1, results[1].second);

        std::cout << "Result for [" << kafka::KafkaMetrics::toString(keys) << "] ==> " << kafka::KafkaMetrics::toString(results) << std::endl;
    }

    {
        const std::vector<std::string> keys = {"topics", "*", "*", "*", "consumer_lag"};
        auto results = metrics.getInt(keys);

        ASSERT_EQ(2, results.size());
        EXPECT_EQ((std::vector<std::string>{"0d08e094-cea41296", "partitions", "0"}), results[0].first);
        EXPECT_EQ(123, results[0].second);
        EXPECT_EQ((std::vector<std::string>{"0d08e094-cea41296", "partitions", "-1"}), results[1].first);
        EXPECT_EQ(-1, results[1].second);

        std::cout << "Result for [" << kafka::KafkaMetrics::toString(keys) << "] ==> " << kafka::KafkaMetrics::toString(results) << std::endl;
    }
}

TEST(KafkaMetrics, ParseProducerMetrics)
{
    kafka::KafkaMetrics metrics(producerMetricsSample);

    // outbuf_latency: Internal request queue latency in microseconds.
    //                 This is the time between a request is enqueued on the transmit (outbuf) queue and the time the request is written to the TCP socket.
    {
        const std::vector<std::string> keys = {"brokers", "*", "outbuf_latency", "avg"};
        auto results = metrics.getInt(keys);

        ASSERT_EQ(3, results.size());

        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29003/2"}, results[0].first);
        EXPECT_EQ(23, results[0].second);
        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29002/1"}, results[1].first);
        EXPECT_EQ(0, results[1].second);
        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29001/0"}, results[2].first);
        EXPECT_EQ(25, results[2].second);

        std::cout << "Result for [" << kafka::KafkaMetrics::toString(keys) << "] ==> " << kafka::KafkaMetrics::toString(results) << std::endl;
    }

    // outbuf_msg_cnt: Number of messages awaiting transmission to broke
    {
        const std::vector<std::string> keys = {"brokers", "*", "outbuf_msg_cnt"};
        auto results = metrics.getInt(keys);

        ASSERT_EQ(3, results.size());

        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29003/2"}, results[0].first);
        EXPECT_EQ(1, results[0].second);
        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29002/1"}, results[1].first);
        EXPECT_EQ(2, results[1].second);
        EXPECT_EQ(std::vector<std::string>{"127.0.0.1:29001/0"}, results[2].first);
        EXPECT_EQ(3, results[2].second);

        std::cout << "Result for [" << kafka::KafkaMetrics::toString(keys) << "] ==> " << kafka::KafkaMetrics::toString(results) << std::endl;
    }
}

TEST(KafkaMetrics, Perf)
{
    using namespace std::chrono;

    auto perfTest = [](const std::function<void()>& cb, const std::string& description) {
        auto timestampBegin = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        cb();
        auto timestampEnd = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

        std::cout << description << " -- takes " << (timestampEnd - timestampBegin) << " ms." << std::endl;
    };

    auto testParse1000Get1000 = [sample = consumerMetricsSample]() {
        for (int i = 0; i < 1000; ++i) {
            kafka::KafkaMetrics metrics(sample);

            auto results = metrics.getInt({"topics", "*", "*", "*", "consumer_lag"});
            EXPECT_EQ(2, results.size());
        }
    };

    auto testParse1Get1000 = [sample = consumerMetricsSample]() {
        kafka::KafkaMetrics metrics(sample);

        for (int i = 0; i < 1000; ++i) {
            auto results = metrics.getInt({"topics", "*", "*", "*", "consumer_lag"});
            EXPECT_EQ(2, results.size());
        }
    };

    perfTest(testParse1000Get1000, "Parse KafkaMetrics 10000 times, and get value 10000 times");
    perfTest(testParse1Get1000, "Parse KafkaMetrics only once, and get value 10000 times");
}

