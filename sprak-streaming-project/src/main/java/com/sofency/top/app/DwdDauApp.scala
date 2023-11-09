package com.sofency.top.app

import com.alibaba.fastjson.JSON
import com.sofency.top.bean.CommonPage
import com.sofency.top.constants.CommonConstant
import com.sofency.top.utils.{CustomKafkaUtils, KafkaOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * <p>Project: big-data-learn - DwdDauApp
 * <p>Powered by echo On 2023-11-09 18:00:32
 * <p> 接收ods中的kafka数据进行处理
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dau")
    //
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topicName = CommonConstant.DWD_PAGE_LOG_TOPIC
    val groupId = "DWD_DAU_GROUP"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaOffsetUtils.getKafkaInputStream(topicName, groupId, streamingContext)

    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 转换成CommonPage
    offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        JSON.parseObject(value, classOf[CommonPage])
      }
    )

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
