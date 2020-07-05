package project.spark.domain

/**
 * 实战课程点击数
 * @param day_search_course 对应hbase中的rowkey,实战课程搜索引擎点击
 * @param click_count 对应访问总数
 *
 * */

case class CourseSearchClickCount (day_search_course: String, click_count: Long)
