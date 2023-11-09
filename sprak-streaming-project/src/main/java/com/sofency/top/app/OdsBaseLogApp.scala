package com.sofency.top.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson2.{JSONArray, JSONObject}
import com.sofency.top.bean.{constructCommonActions, constructCommonDisplays, constructCommonPage, constructCommonStart, parseActions, parseCommon, parseDisplays, parsePage, parseStart}
import com.sofency.top.constants.CommonConstant
import com.sofency.top.utils.{CustomKafkaUtils, KafkaOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * <p>Project: big-data-learn - OdsBaseLogApp
 * <p>Powered by echo On 2023-11-02 16:57:36
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 准备实时环境
    val sparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[2]")
    val stream = new StreamingContext(sparkConf, Seconds(5))

    // 从kafka中消费数据
    var topic: String = "ODS_BASE_LOG_1018"
    var groupId: String = "ODS_BASE_LOG_GROUP_1018"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaOffsetUtils.getKafkaInputStream(topic, groupId, stream)

    // 获取offset偏移量
    var offsetRange: Array[OffsetRange] = null
    val getOffsetRangeDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 打印100条数据
    // 转换数据结构 进行序列化

    getOffsetRangeDStream.map(consumerRecord => {
      val result = consumerRecord.value()
      JSONObject.parseObject(result)
    }).foreachRDD(
      rdd => {
        rdd.foreachPartition(
          objIter => {
            for (obj <- objIter) {
              // 分流过程
              // 分流错误数据
              val errObj: JSONObject = obj.getJSONObject("err")
              if (errObj != null) {
                CustomKafkaUtils.send(CommonConstant.DWD_ERROR_LOG_TOPIC, obj.toJSONString())
              } else {
                // 提取公共数据
                val commonObj: JSONObject = obj.getJSONObject("common")
                val common = parseCommon(commonObj)
                // 时间戳
                val timestamp: Long = obj.getLong("ts")
                val pageObj = obj.getJSONObject("page")
                if (pageObj != null) {
                  val page = parsePage(pageObj)
                  val commonPage = constructCommonPage(common, page, timestamp)
                  // 发送page页面信息
                  CustomKafkaUtils.send(CommonConstant.DWD_PAGE_LOG_TOPIC, JSON.toJSONString(commonPage, new SerializeConfig(false)))
                  // 页面曝光数据
                  val displayJsonArr: JSONArray = obj.getJSONArray("displays")
                  if (displayJsonArr != null && displayJsonArr.size() != 0) {
                    for (i <- 0 until displayJsonArr.size()) {
                      val displays = parseDisplays(displayJsonArr.getJSONObject(i))
                      val commonDisplays = constructCommonDisplays(common, page, displays, timestamp)
                      CustomKafkaUtils.send(CommonConstant.DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(commonDisplays, new SerializeConfig(false)))
                    }
                  }
                  // 事件数据
                  val actionJsonArr: JSONArray = obj.getJSONArray("actions")
                  if (actionJsonArr != null && actionJsonArr.size() != 0) {
                    for (i <- 0 until actionJsonArr.size()) {
                      val actionJsonObj = actionJsonArr.getJSONObject(i)
                      val actions = parseActions(actionJsonObj)
                      val commonActions = constructCommonActions(common, page, actions, timestamp)
                      CustomKafkaUtils.send(CommonConstant.DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(commonActions, new SerializeConfig(false)))
                    }
                  }
                }
                // 启动数据
                val startObj = obj.getJSONObject("start")
                if (startObj != null) {
                  val start = parseStart(startObj)
                  val commonStart = constructCommonStart(common, start, timestamp)
                  CustomKafkaUtils.send(CommonConstant.DWD_START_LOG_TOPIC, JSON.toJSONString(commonStart, new SerializeConfig(false)))
                }
              }
            }

            // 刷新数据落盘 foreachPartition里面，Executor段执行，每个执行窗口(5s)每分区执行一次
            CustomKafkaUtils.flush()
          }
        )
        // 提交偏移量
        KafkaOffsetUtils.saveOffset(topic, groupId, offsetRange)
      }
    )

    // 在哪提交offset, forechRDD里面，Partition外面
    // forechRDD外面，driver端，每次启动程序执行一次
    // forechRDD里面，Partition外面，driver端，一批次执行一次（周期短）
    // 循环 里面，executor端，每条数据执行一次

    // 在哪flush(刷写缓冲区)    foreachPartition里面，循环 外面
    //     forechRDD外面，driver端，分流是在executor端完成，刷写的不是一个对象
    //     forechRDD里面，Partition外面，driver端，forechRDD外面一个道理
    //     foreachPartition里面，循环 外面，executor端，每批次每分区执行一次
    //     循环 里面，executor端，相当于同步发送

    stream.start()
    stream.awaitTermination()
  }
}
