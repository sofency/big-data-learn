#!/bin/bash

# 定义公共变量
kafka_cluster=hadoop100:9092,hadoop101:9092,hadoop102:9092

if [ $# -lt 1 ]
then
    echo "No Args Input"
    exit;
fi

case $1 in
"start")
    echo "===================启动kafka集群========================"
    for i in hadoop100 hadoop101 hadoop102
    do
        echo "================zookeeper $i start======================="
        ssh $i "/opt/module/kafka/bin/kafka-server-start.sh /opt/module/kafka/config/server.properties >>/dev/null 2>&1 &"
    done
;;
"stop")
    echo "===================关闭kafka集群========================"
    for i in hadoop100 hadoop101 hadoop102
    do
        echo "================kafka $i stop======================="
        ssh $i "/opt/module/kafka/bin/kafka-server-stop.sh stop"
    done
;;
kc)
    if [ $2 ]
    then /opt/module/kafka/bin/kafka-consumer.sh --bootstrap-server=$kafka_cluster --topic=$2
    else
        echo "kafka-cluster {start|stop|kc [topic]| kp [topic] | delete [topic] | list}"
    fi
;;
kp)
    if [ $2 ]
    then /opt/module/kafka/bin/kafka-console-producer.sh --broker-list=$kafka_cluster --topic=$2
    else
        echo "kafka-cluster {start|stop|kc [topic]| kp [topic] | delete [topic] | list}"
    fi
;;
list)
    /opt/module/kafka/bin/kafka-topics.sh --list --bootstrap-server $kafka_cluster
;;
describe)
    if [ $2 ]
    then /opt/module/kafka/bin/kafka-topics.sh --describe --bootstrap-server $kafka_cluster --topic $2
    else
        echo "kafka-cluster {start|stop|kc [topic]| kp [topic] | delete [topic] | list}"
    fi
;;
delete)
    if [ $2 ]
    then /opt/module/kafka/bin/kafka-topics.sh --delete --bootstrap-server $kafka_cluster --topic $2
    else
        echo "kafka-cluster {start|stop|kc [topic]| kp [topic] | delete [topic] | list}"
    fi
;;
*)
    echo "error input; example: kafka-cluster {start|stop|kc [topic]| kp [topic]}"
    exit
esac