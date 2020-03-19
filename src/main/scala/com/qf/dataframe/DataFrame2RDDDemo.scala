package com.qf.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Description：DataFrame转换成RDD演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月06日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object DataFrame2RDDDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(DataFrame2RDDDemo.getClass.getSimpleName)
      .getOrCreate()


    //直接读取磁盘上json格式的数据，装载为DataFrame
    val df: DataFrame = spark.read.json("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\json")

    //转换成rdd
    val rdd: RDD[Row] = df.rdd
    rdd.map(row => {
      val age = row.getAs[Long]("age")
      val faceValue = row.getAs[Double]("faceValue")
      val height = row.getAs[Double]("height")
      val name = row.getAs[String]("name")
      val weight = row.getAs[Double]("weight")
      (name, age, faceValue, height, weight + 3)
    }).foreach(println)

    //stop
    spark.stop
  }
}
