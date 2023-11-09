package com.sofency.top.app

import com.alibaba.fastjson.JSON
import com.sofency.top.utils.{CustomKafkaUtils, KafkaOffsetUtils, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

/**
 *
 * <p>Project: big-data-learn - OdsBaseDbApp
 * <p>Powered by echo On 2023-11-08 22:22:33
 * <p>数据库操作数据 binlog => maxwell => kafka=> spark-streaming 消费
 * <p>针对历史数据 maxwell 有引导的功能
 * <p>maxwell-bootstrap --database gmall --table yser_info --config ../config.properties
 * <p>这样初始化数据时的类型为bootstrap-insert
 *
 * <p>现在要对同一条数据的前后操作变化进行修改检测 首先要确保数据更改后变化到达处理端的时间是按顺序的
 * <p>1. maxwell端指定发送到kafka中分区策略
 *       producer_partition_by=column 指定按列进行分区
 *       producer_partition_columns=id 按id进行分区操作
 *       producer_partition_by_fallback=table 如果上述字段不存在则按表进行分区操作
 *
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */
class OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("ods_base")
    val context = new StreamingContext(sparkConf, Seconds(5))

    // 从kafka中消费数据
    var topic: String = "ODS_BASE_DB_1018"
    var groupId: String = "ODS_BASE_DB_GROUP_1018"

    // 读取偏移量
    val offsets: Map[TopicPartition, Long] = KafkaOffsetUtils.readOffset(topic, groupId)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaOffsetUtils.getKafkaInputStream(topic, groupId, context)

    // 提取出偏移量
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    val jsonObjStream = offsetRangesDStream.map(
      // 转换数据结构
      consumerRecord => {
        // 获取到消息的值
        val dataJson: String = consumerRecord.value()
        val jsonObject = JSON.parseObject(dataJson)
        jsonObject
      }
    )

    // 分流
    // 事实表清单
    // val factTables = Array[String]("order_info", "order_detail")
    // 维度表清单
    // val dimTables = Array[String]("user_info", "base_province")


    jsonObjStream.foreachRDD(
      rdd => {

        // 一批次执行一次 从redis中读取清单表 driver 端运行
        // 这些数据在driver端 如果需要在executor端执行。则需要进行序列化传递
        val jedis1 = RedisUtil.getJedisFromPool()
        val factTables: java.util.Set[String] = jedis1.smembers("FACT:TABLES")
        val dimTables: java.util.Set[String] = jedis1.smembers("DIM:TABLES")

        val factTableBrod: Broadcast[util.Set[String]] = context.sparkContext.broadcast(factTables)
        val dimTablesBrod: Broadcast[util.Set[String]] = context.sparkContext.broadcast(dimTables)

        jedis1.close()

        rdd.foreachPartition(
          jsonIter => {
            // 进行取数据
            val jedis = RedisUtil.getJedisFromPool()

            jsonIter.foreach(
              jsonItem => {
                val operateType = jsonItem.getString("type")
                val operateValue = operateType match {
                  case "bootstrap-insert" => "M"
                  case "insert" => "I"
                  case "update" => "U"
                  case "delete" => "D"
                  case _ => null
                }
                // 如果不为空则进行操作
                if (operateValue != null) {
                  // 提取表名
                  val table = jsonItem.getString("table")
                  // 事实数据
                  if (factTableBrod.value.contains(table)) {
                    // 实时数据
                    val dwdTopicName = s"DWD_${table.toUpperCase}_${operateValue}_1018"
                    CustomKafkaUtils.send(dwdTopicName, jsonItem.getString("data"))
                  }
                  // 维度数据 分流到redis中 从广播变量中获取数据
                  if (dimTablesBrod.value.contains(table)) {
                    val dataObj = jsonItem.getJSONObject("data")
                    val id = dataObj.getString("id")
                    val redisKey = s"DIM:${table.toUpperCase}:${id}"
                    // 放到redis中
                    jedis.set(redisKey, dataObj.toString)
                  }
                }
              }
            )

            jedis.close()
            // 刷新Kafka缓冲区
            CustomKafkaUtils.flush()
          }
        )
        //  提交offset
        KafkaOffsetUtils.saveOffset(topic, groupId, offsetRanges)
      }
    )

    context.start()
    context.awaitTermination()
  }
}
