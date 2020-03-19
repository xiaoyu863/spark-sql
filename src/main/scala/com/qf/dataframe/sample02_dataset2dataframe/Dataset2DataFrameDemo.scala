package com.qf.dataframe.sample02_dataset2dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Description：Dataset转换成DataFrame演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object Dataset2DataFrameDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(Dataset2DataFrameDemo.getClass.getSimpleName)
      .getOrCreate()

    val ds: Dataset[String] = spark.read.textFile("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\students.txt")
    val df: DataFrame = ds.toDF()
    df.show

    //stop
    spark.stop
  }
}
