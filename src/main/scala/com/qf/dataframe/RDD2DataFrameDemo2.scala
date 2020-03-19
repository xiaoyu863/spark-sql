package com.qf.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Description：RDD转换成DataFrame演示,样例类方式<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月06日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object RDD2DataFrameDemo2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(RDD2DataFrameDemo2.getClass.getSimpleName)
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //方式2：RDD转换成DataFrame之样例类方式 (通过反射获取Scheam)
    val rdd: RDD[Student] = sc.textFile("file:///F:\\intellij-workplace\\spark-sql\\data\\students.txt")
      .map(perLine => {
        val arr = perLine.split("\\s+")
        Student(arr(0).trim.toInt, arr(1).trim, arr(2).trim.toInt)
      })

    //导入相应的隐式转换方法
    import spark.implicits._

    println("RDD转换成DataFrame之样例类方式\n")
    val df:DataFrame= rdd.toDF
    df.createOrReplaceTempView("people")
    spark.sql("select * from people where id=1").show

    spark.stop

  }
}

/**
  * Student样例类
  *
  * @param id
  * @param name
  * @param ag
  */
case class Student(id: Int, name: String, ag: Int)
