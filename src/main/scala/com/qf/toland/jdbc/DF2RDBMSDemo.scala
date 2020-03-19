package com.qf.toland.jdbc

import java.util.Properties

import com.qf.util.SparkUtil
import org.apache.spark.sql.SaveMode

/**
  * Description：将DataFrame临时表中的数据计算完毕之后落地到RDBMS中目标表中存储起来<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object DF2RDBMSDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(
      DF2RDBMSDemo.getClass.getSimpleName,
      "local[*]")

    //读取db之spark-core-study中的表tb_per_hour_province_top3中的数据
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "88888888")

    spark.read
      .jdbc("jdbc:mysql://xiaoyu1:3306/spark-core-study?useUnicode=true&characterEncoding=utf-8",
        "tb_per_hour_province_top3", properties)

      .createOrReplaceTempView("tb_ad")

    spark.sqlContext.cacheTable("tb_ad")

    //计算（spark集群，或是：hadoop集群）并落地
    spark.sql(
      """
        |select
        |  concat('编号→',id) id,
        |  concat('省份名→',province) province,
        |  concat('时间→',hour) hour,
        |  concat('广告编号→',adId) adId,
        |  concat('点击次数→',cnt) cnt
        |from tb_ad
      """.stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc("jdbc:mysql://NODE03:3306/spark-core-study?useUnicode=true&characterEncoding=utf-8",
        "tb_new_ad_result", properties)

    //stop
    spark.stop()
  }
}
