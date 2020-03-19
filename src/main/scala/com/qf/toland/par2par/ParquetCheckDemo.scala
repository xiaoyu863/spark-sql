package com.qf.toland.par2par

import com.qf.util.SparkUtil
import org.apache.spark.sql.SaveMode

/**
  * Description：读取的源默认是parquest格式，输出的目的地默认也是parquet格式演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ParquetCheckDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark = SparkUtil.getSparkSession(
      ParquetCheckDemo.getClass.getSimpleName,
      "local[*]")

    spark.read
      .load("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\output")
      .createOrReplaceTempView("tb_emp")

    val df = spark.sql(
      """
        |select
        |  *
        |from tb_emp
      """.stripMargin)

    df.show

    //输出，默认的格式也是parquet
    df.write
      .mode(SaveMode.Overwrite)
      .save("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\output3")

    //stop
    spark.stop()
  }
}
