package com.qf.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Description：RDD转换成DataFrame演示,structType方式<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月06日
  *
  * @author 徐文波
  * @version : 1.0
  */
object RDD2DataFrameDemo3 {


  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(RDD2DataFrameDemo2.getClass.getSimpleName)
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //方式3：RDD转换成DataFrame之StructType方式
    val rdd: RDD[Row] = sc.textFile("file:///F:\\intellij-workplace\\spark-sql\\data\\students.txt")
      .map(perLine => {
        val arr = perLine.split("\\s+")
        Row(arr(0).trim.toInt, arr(1).trim, arr(2).trim.toInt)
      })

    val structType: StructType = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("age", IntegerType)
    ))

    val df : DataFrame= spark.createDataFrame(rdd, structType)
    df.createOrReplaceTempView("students_temp")
    df.write.save("hdfs://xiaoyu1:9000/out/00")
    //stop
    spark.stop



  }
}
