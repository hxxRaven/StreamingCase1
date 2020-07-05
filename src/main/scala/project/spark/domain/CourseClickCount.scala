package project.spark.domain
/**
 * 实战课程点击数
 * @param day_course 对应hbase中的rowkey
 * @param click_count 对应访问总数
 *
 * */

case class CourseClickCount (day_course: String, click_count: Long)
