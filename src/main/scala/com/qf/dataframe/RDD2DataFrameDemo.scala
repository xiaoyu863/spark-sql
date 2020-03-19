package com.qf.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DatasetHolder, SparkSession}

/**
  * Description：RDD转换成DataFrame演示,元组方式<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月06日
  *
  * @author 徐文波
  * @version : 1.0
  */
object RDD2DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(RDD2DataFrameDemo.getClass.getSimpleName)
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //方式1：RDD转换成DataFrame之元组方式
    val rdd: RDD[(Int, String, Int)] = sc.textFile("file:///F:\\intellij-workplace\\spark-sql\\data\\students.txt")
      .map(perLine => {
        val arr = perLine.split("\\s+")
        (arr(0).trim.toInt, arr(1).trim, arr(2).trim.toInt)
      })

    //导入相应的隐式转换方法
    import spark.implicits._
    //上述语句将rdd隐式转换为DatasetHolder实例
    rdd.toDF("id", "name", "age").show

    spark.stop

  }
}
