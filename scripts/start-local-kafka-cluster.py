#!/usr/bin/env python3

import signal
import sys
import subprocess
import re
import copy
import json
import argparse
import os
import shutil
import glob
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
        admin.enableServer=false
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
        auto.create.topics.enable=false
    ''')
    properties = brokerTemplate.substitute(broker_id=brokerId, listener_port=brokerPort, zookeeper_port=zookeeperPort, log_dir=logDir)
    return properties

################################################################################

def StartZookeeperServer(name, propFile, outDir):
    cmd = '{0} {1}'.format(ZOOKEEPER_SERVER_START_BIN, propFile)
    processPool.addProcess(cmd, name, os.path.join(outDir, name+'.out'), os.path.join(outDir, name+'.err'))

def StartKafkaServer(name, propFile, outDir):
    cmd = '{0} {1}'.format(KAFKA_SERVER_START_BIN, propFile)
    processPool.addProcess(cmd, name, os.path.join(outDir, name+'.out'), os.path.join(outDir, name+'.err'))

################################################################################

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--zookeeper-port', help='The port for zookeeper', required=True)
    parser.add_argument('--broker-ports', nargs='+', help='The ports for kafka brokers', required=True)
    parser.add_argument('--temp-dir', help='The location for kafka/zookeeper log files, console printout, etc', required=True)
    parsed = parser.parse_args()

    zookeeperPort = parsed.zookeeper_port
    brokerPorts   = parsed.broker_ports

    if os.path.exists(parsed.temp_dir):
        shutil.rmtree(parsed.temp_dir)

    logDir  = os.path.join(parsed.temp_dir, 'log')
    outDir  = os.path.join(parsed.temp_dir, 'out')
    propDir = os.path.join(parsed.temp_dir, 'properties')

    PropFile = namedtuple('PropertiesFile', 'filename context')

    # Generate properties files
    zookeeperPropFiles = []
    zookeeperPropFiles.append(PropFile(os.path.join(propDir, 'zookeeper.properties'), GenerateZookeeperConfig(zookeeperPort, os.path.join(logDir, 'zookeeper'))))
    kafkaPropFiles = []
    for (i, brokerPort) in enumerate(brokerPorts):
        kafkaPropFiles.append(PropFile(os.path.join(propDir, 'kafka{0}.properties'.format(i)), GenerateBrokerConfig(i, brokerPort, zookeeperPort, os.path.join(logDir, 'kafka{0}'.format(i)))))

    os.makedirs(propDir)
    for propFile in (set(zookeeperPropFiles) | set(kafkaPropFiles)):
        with open(propFile.filename, 'w') as f:
              f.write(propFile.context)

    os.makedirs(outDir)

    StartZookeeperServer('zookeeper', zookeeperPropFiles[0].filename, outDir)

    time.sleep(5)

    for (i, brokerPort) in enumerate(brokerPorts):
        StartKafkaServer('kafka{0}'.format(i), kafkaPropFiles[i].filename, outDir)

    MAX_RETRY = 60
    retry = 0
    while retry < MAX_RETRY:
        time.sleep(1)
        kafkaBrokerPids = []

        for brokerPort in brokerPorts:
            cmd = 'lsof -nP -iTCP:{0} | grep LISTEN'.format(brokerPort)
            cmdCall = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            (out, err) = cmdCall.communicate();
            matched = re.search('[^\s-]+ +([0-9]+) +.*', out.decode('utf-8'))
            if matched:
                kafkaBrokerPids.append(matched.group(1))

        if len(kafkaBrokerPids) != len(brokerPorts):
            retry += 1
            continue

        with open(r'test.env', 'w') as envFile:
            envFile.write('export KAFKA_BROKER_LIST={0}\n'.format(','.join(['127.0.0.1:{0}'.format(port) for port in brokerPorts])))
            envFile.write('export KAFKA_BROKER_PIDS={0}\n'.format(','.join([pid for pid in kafkaBrokerPids])))
            break

    if retry < MAX_RETRY:
        print('Kafka cluster started with ports: {0}!'.format(brokerPorts))
        processPool.run()
    else:
        print('Kafka cluster failed to start with ports: {0}!'.format(brokerPorts))
        processPool.terminate()
        for filename in glob.glob(os.path.join(outDir, '*')):
            with open(filename, 'r') as f:
                print('^^^^^^^^^^ {0} ^^^^^^^^^^'.format(os.path.basename(filename)))
                print(f.read())
                print('vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv')

if __name__ == '__main__':
    main()

