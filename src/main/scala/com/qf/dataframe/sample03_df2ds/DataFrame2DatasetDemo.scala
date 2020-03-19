package com.qf.dataframe.sample03_df2ds

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Description：DataFrame→Dataset<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object DataFrame2DatasetDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(DataFrame2DatasetDemo.getClass.getSimpleName)
      .getOrCreate()

    val df: DataFrame = spark.read.json("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\json")

    import spark.implicits._

    //DataFrame→Dataset
    //注意点：
    // ①json文件中的int类型的值，要对应样例类中的BigInt类型的属性。
    //②导入spark.implicits._，进行相应的编码操作
    //③样例类中属性的顺序和json文件中key的顺序可以不同，但是名字要保持一致。
    val ds: Dataset[Person] = df.as[Person]

    ds.show

    //stop
    spark.stop
  }
}


/**
  * Person样例类
  *
  * @param name
  * @param age
  * @param faceValue
  * @param height
  * @param weight
  */
case class Person(name: String, age: BigInt, faceValue: Double, height: Double, weight: Double)
