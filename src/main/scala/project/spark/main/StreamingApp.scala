package project.spark.main

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import project.spark.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import project.spark.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import project.spark.util.DateUtils

object StreamingApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Streaming")
    val ssc = new StreamingContext(conf, Seconds(60))

    //组名
    val group = "group1"
    //主题
    val topic = "streamingtopic"
    //Kafaka链接
    val brokerlist = "192.168.226.130:9092,192.168.226.131:9092,192.168.226.132:9092"

    //创建将要使用的topic集合
    val topics = Array(topic)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerlist,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    var offsetRanges = Array[OffsetRange]()

    kafkaStream.foreachRDD { kafkaRDD =>
      //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
      offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      val lines: RDD[String] = kafkaRDD.map(_.value())
      //对RDD进行操作，触发Action
      lines.foreachPartition(partition =>
        partition.foreach(
          x => {
          //数据清洗-获取课程编号
          //143.87.98.55    2020-06-27 10:57:01     "GET /class/145.html HTTP/1.1"  500     https://www.so.com/s?p=Spark Streaming实战
          val infos = x.split("\t")
          // /class/145.html
          val url: String = infos(2).split(" ")(1)
          var courseId = 0
          if (url.startsWith("/class")) {
            // 145.html
            val courseIdHTML: String = url.split("/")(2)
            // 145
            courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
          }
          if (courseId != 0) {
            //功能2 包装后的数据记录数据库
            val log: ClickLog = ClickLog(infos(0), DateUtils.parse(infos(1)), courseId, infos(3).toInt, infos(4))
            val count: CourseClickCount = CourseClickCount(log.time.substring(0, 8) + "_" + log.courseId, 1)
            CourseClickCountDAO.save(count)

            //功能2 统计今天到现在为止实战课程，搜索引擎过来的访问量
            val refer: String = log.refer
            val refer_split: Array[String] = refer.replaceAll("//", "/").split("/")
            var host: String = ""
            if (refer_split.length > 2) {
              host = refer_split(1)
              val count1: CourseSearchClickCount = CourseSearchClickCount(log.time + "_" + host + "_" + courseId, 1)
              CourseSearchClickCountDAO.save(count1)
            }
          }
        })
      )
      //偏移量同步回Kafka
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
