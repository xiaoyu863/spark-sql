package com.qf.toland.parquet

import com.qf.util.SparkUtil
import org.apache.spark.sql.SaveMode

/**
  * Description：需求：读取json格式的数据到内存中的DataFrame中，处理后，以parquet格式落地到磁盘上。<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object DF2ParquetDemo {
  def main(args: Array[String]): Unit = {
    //SparkSession
    val spark = SparkUtil.getSparkSession(
      DF2ParquetDemo.getClass.getSimpleName,
      "local[*]")

    spark.read
      .json("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\function\\employees.json")
      .createOrReplaceTempView("tb_emp")


    val df = spark.sql(
      """
        |select
        |  id,
        |   name,
        |   age+1 `age`,
        |   salary+500 `salary`,
        |   concat('中国',address) `address`
        |from tb_emp
      """.stripMargin)


    //本地测试
    df.show

    //落地
    df.write.mode(SaveMode.Overwrite).parquet("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\output")
    //stop
    spark.stop()
  }
}
