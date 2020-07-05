package project.spark.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 时间处理工具
 * */

object DateUtils {
  val YYYYMMDDHHMMSS_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGE_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def parse(time: String) = {
    TARGE_FORMAT.format(new Date(getTime(time)))
  }

  def getTime(time: String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def main(args: Array[String]): Unit = {
    println(parse("2020-06-27 12:11:11"))
  }
}
