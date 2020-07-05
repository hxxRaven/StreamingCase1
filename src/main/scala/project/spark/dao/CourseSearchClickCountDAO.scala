package project.spark.dao

import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import project.spark.domain.{CourseClickCount, CourseSearchClickCount}
import spark.project.utils.HBaseUtils

/**
 * 实战课程点击数 数据访问层
 *
 *
 * */

object CourseSearchClickCountDAO {
  val tabelName = "course_search_clickcount"
  val cf  = "info"
  val qualifer = "click_count"


  /**
   * 保存数据到Hbase
   *
   * */
  def save(cord: CourseSearchClickCount) = {

    val table = HBaseUtils.getInstance().getTable(tabelName)
    //rowkey， cf， qualifer
    //increment 自动相加
    table.incrementColumnValue(Bytes.toBytes(cord.day_search_course), Bytes.toBytes(cf), Bytes.toBytes(qualifer), cord.click_count)


  }
  /**
   * 根据rowkey查询值
   * */
  def count(day_course: String) = {
    val tabel = HBaseUtils.getInstance().getTable(tabelName)
    val get = new Get(Bytes.toBytes(day_course))
    val value = tabel.get(get).getValue(cf.getBytes, qualifer.getBytes)

    //防止第一次操作，没有值出错
    if (value == null) {
      0L
    }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val v1 = CourseSearchClickCount("20181111_www.baidu.com_8", 8)
    save(v1)
  }

}
