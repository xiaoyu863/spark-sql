package com.qf.lacsql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description：基站案例之Spark SQL版演示，sql合并版<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object LacDemo2 {
  def main(args: Array[String]): Unit = {

    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(LacDemo2.getClass.getSimpleName)
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //导入
    import spark.implicits._

    //    思路：
    //    1，读取日志文件文件，装载到内存中的RDD→DataFrame→tb_log
    sc.textFile("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\mobilelocation\\log")
      .map(perLine => {
        val arr = perLine.split(",")
        (arr(0).trim, arr(1).trim.toLong, arr(2).trim, arr(3).trim.toInt)
      }).toDF("phone", "time", "lacId", "flg")
      .createOrReplaceTempView("tb_log")

    //优化之缓存：将经常使用到的表缓存起来
    spark.sqlContext.cacheTable("tb_log")

    //    2,针对tb_log进行分析，求出：不同用户停留时长最常的两个基站
    //对虚拟表tb_log2分组求top2
    spark.sql(
      """
        |select
        |  phone,
        |  lacId,
        |  stayTime
        |from(
        |   select
        |       *,
        |       row_number() over(distribute by phone sort by stayTime desc) level
        |   from (
        |       select
        |        phone,
        |        lacId,
        |        sum(case flg when 0 then time  else -time end) stayTime
        |       from tb_log
        |       group by phone,lacId
        |   )t1
        |) t2 where t2.level<=2
      """.stripMargin)
      .createOrReplaceTempView("tb_log")

    //    3,读取基站文件，装载到内存中的RDD→DataFrame→tb_lac
    sc.textFile("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\mobilelocation\\lac_info.txt")
      .map(perLine => {
        val arr = perLine.split(",")
        (arr(0).trim, arr(1).trim, arr(2).trim)
      }).toDF("lacId", "jd", "wd")
      .createOrReplaceTempView("tb_lac")


    //    4,上述两个表进行内连接查询，得出最终的结果
    println("最终结果是→ ")
    spark.sqlContext
      .sql(
        """
          |select
          |    log.phone `手机号码`,
          |    log.lacId `基站id`,
          |    lac.jd `经度`,
          |    lac.wd `纬度`,
          |    log.stayTime `停留时长`
          | from tb_log log, tb_lac lac
          |where log.lacId = lac.lacId
        """.stripMargin)
      .show

    //stop
    spark.stop
  }
}
