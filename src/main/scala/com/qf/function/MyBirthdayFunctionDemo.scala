package com.qf.function

import com.qf.util.{GetYearAnimal, GetYearStar}
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
object MyBirthdayFunctionDemo {


  def main(args: Array[String]): Unit = {
    //需求： 读取雇员的信息，使用自定义函数，求出每个员工名字的长度。
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(MyBirthdayFunctionDemo.getClass.getSimpleName)
      .getOrCreate()

    spark.read
      .json("file:///F:\\intellij-workplace\\spark-sql\\data\\function\\employees.json")
      .createOrReplaceTempView("tb_emp")

    //注册自定义函数
    //函数体简单
    //spark.udf.register[Int, String]("getLen", (name: String) => name.length)

    //下述的写法适合于：函数体处理复杂，为了增强源码的可读性，建议使用方法进行封装
    spark.udf.register[String, String ,Int]("getInfo", (year: String,which:Int) => getMyInfo(year,which))
  //  spark.udf.register[String, String]("getStar", (ym: String) => getMyStar(ym))

    val df1 = spark.sql(
      """
        |select
        | name,
        | getInfo(age,0)
        | from tb_emp
      """.stripMargin)
    df1.createOrReplaceTempView("tmp1")

    val df2 = spark.sql(
      """
        |select
        | name,
        | getInfo(age,1) x
        | from tb_emp
      """.stripMargin)
    df2.createOrReplaceTempView("tmp2")

    spark.sql(
      """
        | select tmp1.*,tmp2.x `星座` from
        | tmp1,tmp2
        | where
        | tmp1.name=tmp2.name
        |""".stripMargin
    ).show

    //stop
    spark.stop
  }


  /**
    * 根据参数传入的字段名，给出字段的长度
    *
    * @param year
    * @return
    */
  def getMyBirthday(year: String): String = {
    GetYearAnimal.getYear(2019-Integer.parseInt(year))
  }

  /**
   * 根据参数传入的字段名，给出字段的长度
   *
   *
   * @return
   */
  def getMyStar(age: String): String = {
    val mAge = Integer.parseInt(age)
    GetYearStar.getStar("12-05")
  }

  def getMyInfo(age:String,which:Int):String={
    if(which==1) getMyStar(age)
    else getMyBirthday(age)
  }
}
