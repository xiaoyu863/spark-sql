package com.qf.function

import org.apache.spark.sql.SparkSession

/**
  * Description：Spark sql常用函数演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SparkSQLCommonFunctionDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(SparkSQLCommonFunctionDemo.getClass.getSimpleName)
      .getOrCreate()

    //需求：求出公司中员工的最高薪水，最低薪水，平均薪水，员工总数以及工资总额。
    //①读取json文件到临时表中，并映射为一张虚拟表tb_emp
    spark.read.json("file:///F:\\intellij-workplace\\spark-sql\\data\\function\\employees.json")
        .createOrReplaceTempView("tb_emp")

  //②使用sql分析
    spark.sql(
      """
        |select
        |  max(salary) `最高薪水`,
        |  min(salary) `最低薪水`,
        |  avg(salary) `平均薪水`,
        |  count(id) `员工总数`,
        |  sum(salary) `工资总额`
        |from tb_emp
      """.stripMargin)
        .show

    //stop
    spark.stop
  }
}
