package com.qf.toland.parquet2json

import com.qf.util.SparkUtil
import org.apache.spark.sql.SaveMode

/**
  * Description：读取磁盘上parquet格式的资源到DataFrame中，计算完毕后，以json格式存储起来<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object Parquet2JsonDemo extends App {
  //SparkSession
  val spark = SparkUtil.getSparkSession(
    Parquet2JsonDemo.getClass.getSimpleName,
    "local[*]")

  spark.read.
    parquet("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\output")
    .createOrReplaceTempView("tb_emp")

  val df = spark.sql(
    """
      |select
      |  id,
      |   name,
      |   age+1 `age`,
      |   salary+1000 `salary`,
      |   concat('亚洲中国',address) `address`
      |from tb_emp
    """.stripMargin)

  df.write
    .mode(SaveMode.Overwrite)
    .format("json")
    .save("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\output2")

  //stop
  spark.stop()
}
