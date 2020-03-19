package com.qf.sparksession

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Description：SparkSession实例创建方式演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月06日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SparkSessionCreateWay {
  def main(args: Array[String]): Unit = {

    //SparkSession实例的三中创建方式：
    //方式1：Builder构建器方式
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(SparkSessionCreateWay.getClass.getSimpleName)
      .getOrCreate()

    println(s"Builder构建器方式创建的SparkSession实例是：$spark")

    spark.stop()

    println("\n___________________________________________\n")
    //方式2：SparkConf构建方式
    val conf: SparkConf = new SparkConf()
    conf.setAppName(SparkSessionCreateWay.getClass.getSimpleName)
      .setMaster("local[*]")

    val spark2: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    println(s"SparkConf构建方式创建的SparkSession实例是：$spark2")

    spark2.stop()


    println("\n___________________________________________\n")
    //方式3：enableHiveSupport构建方式,启用对hive表的支持
    val spark3: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(SparkSessionCreateWay.getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate()

    println(s"enableHiveSupport构建方式创建的SparkSession实例是：$spark3")

    spark3.stop()

  }
}
