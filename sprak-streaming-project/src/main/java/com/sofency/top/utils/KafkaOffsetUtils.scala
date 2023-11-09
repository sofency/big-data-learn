package com.sofency.top.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.StreamInput
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

/**
 *
 * <p>Project: big-data-learn - KafkaOffsetUtils
 * <p>Powered by echo On 2023-11-03 22:52:37
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */
object KafkaOffsetUtils {

  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.length > 0) {
      var offsets: java.util.HashMap[String, String] = new util.HashMap[String, String]()
      // 将各个分区的offset进行统计
      for (elem <- offsetRanges) {
        val partition = elem.partition
        val offset: Long = elem.untilOffset
        offsets.put(partition.toString, offset.toString)
      }
      println("提交offset", offsets)
      val jedis: Jedis = RedisUtil.getJedisFromPool()
      jedis.hset(s"offset:$topic:$groupId", offsets)
      jedis.close()
    }
  }

  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis = RedisUtil.getJedisFromPool()
    // 获取key value
    val redisKey = s"offset:$topic:$groupId"
    val offsets: java.util.Map[String, String] = jedis.hgetAll(redisKey)
    println("读取offset", offsets)
    var results: mutable.Map[TopicPartition, Long] = mutable.Map()
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsets.asScala) {
      val tp: TopicPartition = new TopicPartition(topic, partition.toInt)
      results.put(tp, offset.toLong)
    }
    jedis.close()
    results.toMap
  }

  def getKafkaInputStream(topic: String, groupId: String, context: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    var kafkaDauInputStream: InputDStream[ConsumerRecord[String, String]] = null
    val offsets = readOffset(topic, groupId)
    if (offsets != null && offsets.nonEmpty) {
      kafkaDauInputStream = CustomKafkaUtils.consume(context, topic, groupId, offsets)
    } else {
      kafkaDauInputStream = CustomKafkaUtils.consume(context, topic, groupId)
    }
    kafkaDauInputStream
  }
}
