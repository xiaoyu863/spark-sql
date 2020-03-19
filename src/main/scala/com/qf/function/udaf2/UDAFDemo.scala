package com.qf.function.udaf2

import com.qf.util.SparkUtil
import org.apache.spark.sql.Dataset

/**
  * Description：用户自定义聚合函数之Aggregator方式演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object UDAFDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark = SparkUtil.getSparkSession(
      UDAFDemo.getClass.getSimpleName,
      "local[*]"
    )

    import spark.implicits._

    val ds: Dataset[Emp] = spark.read
      .json("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\function\\employees.json")
      .as[Emp]

    val tc = new MyAvg().toColumn.name("→ 平均薪资 ←")

    ds.select(tc)
      .show

    //stop
    spark.stop()
  }
}
