#!/usr/bin/env python3

import signal
import sys
import subprocess
import re
import copy
import json
import argparse
import os
import time
from multiprocessing import Process
from string import Template
from collections import namedtuple

ZOOKEEPER_SERVER_START_BIN = 'zookeeper-server-start.sh'
KAFKA_SERVER_START_BIN = 'kafka-server-start.sh'


################################################################################

zookeeperPids = []
kafkaPids = []

class ProcessPool(object):
    def __init__(self):
        self.processList = []

    def addProcess(self, cmd, name, outFile, errFile):
        out = open(outFile, 'w')
        err = open(errFile, 'w')
        p = subprocess.Popen(cmd,
                             shell=True,
                             stdin=subprocess.PIPE,
                             stdout=out,
                             stderr=err)

        if 'kafka' in name:
            kafkaPids.append(p.pid)
        else:
            zookeeperPids.append(p.pid)

        self.processList.append((p, name))

    def run(self):
        anyFailure = False
        while self.processList:
            for (i, (p, name)) in enumerate(self.processList):
                ret = p.poll()
                if ret != None:
                    print('Failed to start server: {0}, pid: {1}, ret: {2}'.format(name, p.pid, ret))
                    self.processList.pop(i)
                    anyFailure = True
                    break
        if anyFailure:
            self.terminate()

    def terminate(self):
        for (p, name) in self.processList:
            p.kill()
            print('{0} terminated'.format(name))

    def __del__(self):
        self.terminate()

processPool=ProcessPool()

################################################################################

def GenerateZookeeperConfig(zookeeperPort, dataDir):
    zookeeperTemplate = Template('''
        dataDir=${data_dir}
        clientPort=${port}
    ''')
    properties = zookeeperTemplate.substitute(data_dir=dataDir, port=zookeeperPort)
    return properties

def GenerateBrokerConfig(brokerId, brokerPort, zookeeperPort, logDir):
    brokerTemplate = Template('''
        broker.id=${broker_id}
        listeners=PLAINTEXT://127.0.0.1:${listener_port}
        log.dirs=${log_dir}
        zookeeper.connect=127.0.0.1:${zookeeper_port}
        num.partitions=5
        default.replication.factor=3
        offsets.topic.replication.factor=3
        offsets.commit.timeout.ms=10000
        unclean.leader.election.enable=false
        min.insync.replicas=2
    ''')
    properties = brokerTemplate.substitute(broker_id=brokerId, listener_port=brokerPort, zookeeper_port=zookeeperPort, log_dir=logDir)
    return properties

################################################################################

def StartZookeeperServer(name, propFile, outDir):
    cmd = '{0} {1}'.format(ZOOKEEPER_SERVER_START_BIN, propFile)
    processPool.addProcess(cmd, name, '{0}/{1}.out'.format(outDir, name), '{0}/{1}.err'.format(outDir, name))

def StartKafkaServer(name, propFile, outDir):
    cmd = '{0} {1}'.format(KAFKA_SERVER_START_BIN, propFile)
    processPool.addProcess(cmd, name, '{0}/{1}.out'.format(outDir, name), '{0}/{1}.err'.format(outDir, name))

################################################################################

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--zookeeper-port', help='The port for zookeeper', required=True)
    parser.add_argument('--broker-ports', nargs='+', help='The ports for kafka brokers', required=True)
    parser.add_argument('--log-dir', help='The location for kafka log files', required=True)
    parser.add_argument('--output-dir', help='The location for console printout logging files of zookeeper/brokers', required=True)
    parsed = parser.parse_args()

    zookeeperPort = parsed.zookeeper_port
    brokerPorts = parsed.broker_ports

    logDir = parsed.log_dir
    outDir = parsed.output_dir
    currentDir = os.getcwd()

    PropFile = namedtuple('PropertiesFile', 'filename context')
    # Generate properties files
    propDir = '{0}/properties'.format(currentDir)
    zookeeperPropFiles = []
    zookeeperPropFiles.append(PropFile('{0}/zookeeper.properties'.format(propDir), GenerateZookeeperConfig(zookeeperPort, '{0}/{1}'.format(logDir, 'zookeeper'))))
    kafkaPropFiles = []
    for (i, brokerPort) in enumerate(brokerPorts):
        kafkaPropFiles.append(PropFile('{0}/kafka{1}.properties'.format(propDir, i), GenerateBrokerConfig(i, brokerPort, zookeeperPort, '{0}/kafka{1}'.format(logDir, i))))

    os.makedirs(propDir, exist_ok=True)
    for propFile in (set(zookeeperPropFiles) | set(kafkaPropFiles)):
        with open(propFile.filename, 'w') as f:
              f.write(propFile.context)

    os.makedirs(outDir, exist_ok=True)

    StartZookeeperServer('zookeeper', zookeeperPropFiles[0].filename, outDir)

    time.sleep(5)

    for (i, brokerPort) in enumerate(brokerPorts):
        StartKafkaServer('kafka{0}'.format(i), kafkaPropFiles[i].filename, outDir)

    MAX_RETRY = 60
    retry = 0
    while retry < MAX_RETRY:
        time.sleep(1)

        kafkaBrokerPids = []
        netstatCall = subprocess.Popen(['netstat', '-tlp'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (out, err) = netstatCall.communicate();
        for brokerPort in brokerPorts:
            matched = re.search('tcp[4 6] +[0-9]+ +[0-9]+ +localhost:{0} +.+ +LISTEN *([0-9]+)/java.*'.format(brokerPort), out.decode('utf-8'))
            if matched:
                kafkaBrokerPids.append(matched.group(1))

        if len(kafkaBrokerPids) != len(brokerPorts):
            continue

        with open(r'test.env', 'w') as envFile:
            envFile.write('export KAFKA_BROKER_LIST={0}\n'.format(','.join(['127.0.0.1:{0}'.format(port) for port in brokerPorts])))
            envFile.write('export KAFKA_BROKER_PIDS={0}\n'.format(','.join([pid for pid in kafkaBrokerPids])))
            break

        retry += 1

    if retry < MAX_RETRY:
        print('Kafka cluster started with ports: {0}!'.format(brokerPorts))
        processPool.run()
    else:
        print('Kafka cluster failed to start with ports: {0}!'.format(brokerPorts))
        processPoll.terminate()


if __name__ == '__main__':
    main()

