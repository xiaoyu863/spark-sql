package com.qf.dataframe.sample01_rddanddataset

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Description：Dataset→RDD<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object Dataset2RDDDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(Dataset2RDDDemo.getClass.getSimpleName)
      .getOrCreate()

    val ds: Dataset[String] = spark.read.textFile("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\students.txt")
    ds.printSchema()

    println("\n______________\n")
    //show方法默认显示Dataset数据集中的前20行数据
    ds.show

    println("\n______________\n")
    //Dataset→RDD
    val rdd: RDD[String] = ds.rdd
    rdd.foreach(println)


    //stop
    spark.stop
  }
}
