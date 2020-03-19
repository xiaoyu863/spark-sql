package com.qf.function.udaf

import org.apache.spark.sql.SparkSession

/**
  * Description：用户自定义聚合函数之UserDefinedAggregateFunction方式演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object UserDefinedAggregateFunctionDemo {
  def main(args: Array[String]): Unit = {
    //需求： 读取雇员的信息，使用自定义聚合函数，求出所有员工的平均薪资。
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(UserDefinedAggregateFunctionDemo.getClass.getSimpleName)
      .getOrCreate()

    spark.read
      .json("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\function\\employees.json")
      .createOrReplaceTempView("tb_emp")

    //注册自定义函数

    spark.udf.register("myselfAvg", new MyAvg)

    spark.sql(
      """
        |select
        | count(id) `员工总数`,
        | avg(salary) `内置→平均薪水`,
        | myselfAvg(salary) `自定义→平均薪水`
        |from tb_emp
      """.stripMargin)
      .show

    //stop
    spark.stop
  }
}
