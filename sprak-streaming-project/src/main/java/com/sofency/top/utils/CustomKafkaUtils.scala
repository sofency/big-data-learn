package com.sofency.top.utils

import com.sofency.top.constants.ConfigConstant
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
 *
 * <p>Project: big-data-learn - KafkaUtils
 * <p>Powered by echo On 2023-10-31 22:59:06
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */
object CustomKafkaUtils {
  private val kafkaParam: mutable.Map[String, Object] = mutable.Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ResourceUtils.apply(ConfigConstant.KAFKA_BOOT_SERVER), // 集群地址
    // 设置了消费组，则同一组内只有一个消费着可以获得该消息
    ConsumerConfig.GROUP_ID_CONFIG -> "default",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> ResourceUtils.apply(ConfigConstant.KEY_DESERIALIZER), // key反序列化
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> ResourceUtils.apply(ConfigConstant.VALUE_DESERIALIZER), // value反序列化

    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> ResourceUtils.apply(ConfigConstant.ENABLE_AUTO_COMMIT), // 允许自动提交
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> ResourceUtils.apply(ConfigConstant.AUTO_COMMIT_INTERVAL_MS), // 多少毫秒自动提交
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ResourceUtils.apply(ConfigConstant.AUTO_OFFSET_RESET), // offset重置, 1.没有消费记录 2.最早earliest，最晚latest 默认最晚 最高会出现重复消费的问题
  )

  /**
   * 创建生产者
   *
   * @return
   */
  private def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ResourceUtils.apply(ConfigConstant.KAFKA_BOOT_SERVER))
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ResourceUtils.apply(ConfigConstant.KEY_SERIALIZER))
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ResourceUtils.apply(ConfigConstant.VALUE_SERIALIZER))
    // ack 配置   收到所有集群的ack
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, ResourceUtils.apply(ConfigConstant.ACK))
    // 幂等性配置 只会维持当前会话的幂等性
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ResourceUtils.apply(ConfigConstant.ENABLE_IDEMPOTENCE))

    val producer = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  var producer: KafkaProducer[String, String] = createProducer()


  /**
   * 消费
   * 默认情况下，向具有多个分区的Kafka topic中写入数据时，每个分区的数据是不一样的。
   *
   * @param streamingContext 上下文
   * @param topic            主题
   * @param group            组
   * @return
   */
  def consume(streamingContext: StreamingContext, topic: String, group: String): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    // 返回消费的流
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams = kafkaParam)
    )
  }

  /**
   * 使用自定义的offset
   *
   * @param streamingContext
   * @param topic
   * @param group
   * @param offset
   * @return
   */
  def consume(streamingContext: StreamingContext, topic: String, group: String, offset: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    // 返回消费的流
    KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaParams = kafkaParam, offset)
    )
  }

  /**
   * 按照黏性分区策略
   *
   * @param topic 主题
   * @param msg   消息
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 按照key 进行分区
   */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }


  /**
   * 刷新数据落盘
   */
  def flush(): Unit = {
    producer.flush()
  }
}
