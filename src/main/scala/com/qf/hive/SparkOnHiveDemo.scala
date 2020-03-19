package com.qf.hive

import org.apache.spark.sql.SparkSession

/**
  * Description：spark on hive演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SparkOnHiveDemo extends App {

  //SparkSession
  val spark: SparkSession = SparkSession.builder()
    .master("spark://xiaoyu1:7077")
    .appName(SparkOnHiveDemo.getClass.getSimpleName)
    .enableHiveSupport() //启用对hive的支持
    .getOrCreate()

  //读取hive表数据
  //db名：mtbap_dm
  //table名：dm_user_visit

  spark.sql("create database df_ttt2")

  spark.sql(
    """
      |select
      |  *
      |from test.student
      |limit 10
  """.stripMargin)
    .show

  //stop
  spark.stop
}
