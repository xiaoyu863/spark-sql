package com.qf.dsl_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Description：SparkSQL编程方式1之DSL风格演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object DSLDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(DSLDemo.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    //使用dsl风格的语法
    val df: DataFrame = spark.read
      .json("file:///F:\\intellij-workplace\\spark-sql\\data\\json")

    //需求①： 求出所有成年的学生的信息，显示这些学生的名字和年龄。
    df.filter($"age" >= 18)
      .select("name", "age")
      .show

    println("\n_______________________________\n")

    //需求②： 求出班上不同年龄段学生的总人数。
    df.groupBy("age")
      .count()
      .show()

    println("\n_______________________________\n")

    //需求③： 显示临时表的元数据信息
    df.printSchema


    //stop
    spark.stop
  }
}
