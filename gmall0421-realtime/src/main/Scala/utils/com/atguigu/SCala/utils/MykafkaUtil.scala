package utils.com.atguigu.SCala.utils

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


//Sparlstreaming消费kafka工具类
object MykafkaUtil {
  def main(args: Array[String]): Unit = {
    //指定名字 Master（master三种方式 local 单线程 ， local[*]和服务器线程数有关  ， local[n] 指定线程数   ）
    //选用local[n]  local[4] 因为kafka分区是4 所以设置4个
    val conf: SparkConf = new SparkConf().setAppName("MykafkaUtil").setMaster("local[4]")
    //第一个参数配置 第二个参数采集周期
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    //用KafkaUtils.createDirectStream
    KafkaUtils.createDirectStream(
      //SparkStreaming上下文
      ssc,
      //位置策略
      //LocationStrategies.PreferBrokers：executor和kafka在同一个节点上使用
      // LocationStrategies.PreferConsistent 大多数情况下使用，将在所有执行器上一致地分配分区
      //  LocationStrategies.PreferFixed如果负载不均匀，可以使用此方法在特定主机上放置特定的主题分区。
      //                                映射中未指定的任何TopicPartition都将使用一致的位置。
      //

      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String,String](Array("gmall0421_start"),kafkaParams)
    )
  }

}
