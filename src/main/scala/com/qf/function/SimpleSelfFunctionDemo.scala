package com.qf.function

import org.apache.spark.sql.SparkSession

/**
  * Description：自定义简单函数演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SimpleSelfFunctionDemo {


  def main(args: Array[String]): Unit = {
    //需求： 读取雇员的信息，使用自定义函数，求出每个员工名字的长度。
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(SimpleSelfFunctionDemo.getClass.getSimpleName)
      .getOrCreate()

    spark.read
      .json("file:///F:\\intellij-workplace\\spark-sql\\data\\function\\employees.json")
      .createOrReplaceTempView("tb_emp")

    //注册自定义函数
    //函数体简单
    //spark.udf.register[Int, String]("getLen", (name: String) => name.length)

    //下述的写法适合于：函数体处理复杂，为了增强源码的可读性，建议使用方法进行封装
    spark.udf.register[Int, String]("getLen", (name: String) => getAnyFieldLen(name))

    spark.sql(
      """
        |select
        |   getLen(name) len,
        |   count(*)
        |from tb_emp
        |group by len
      """.stripMargin)
      .show

    //stop
    spark.stop
  }


  /**
    * 根据参数传入的字段名，给出字段的长度
    *
    * @param name
    * @return
    */
  def getAnyFieldLen(name: String): Int = {
    //代码多。。。。
    name.length
  }
}
