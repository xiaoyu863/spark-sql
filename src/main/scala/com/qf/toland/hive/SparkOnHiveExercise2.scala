package com.qf.toland.hive

import com.qf.util.SparkUtil
import org.apache.spark.sql.SparkSession

/**
  * Description：Spark On Hive综合案例演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SparkOnHiveExercise2 {
  def main(args: Array[String]): Unit = {
    //    SparkSession
    //SparkSession
    val spark =     SparkSession.builder()
      .appName(SparkOnHiveExercise2.getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate

    //    初始化
    spark.sql("drop database if exists spark_on_hive_exercise cascade")

    //    建库（与此同时建立与hdfs上资源的映射关系）
    spark.sql("create database spark_on_hive_exercise")

    //    建表
    spark.sql(
      """
        |create table spark_on_hive_exercise.tb_emp(
        | name string,
        | age int,
        | isMarried boolean,
        | deptNo int
        |) row format delimited
        | fields terminated by ','
        | location 'hdfs://ns1/spark-sql/emp'
      """.stripMargin)

    spark.sqlContext.cacheTable("spark_on_hive_exercise.tb_emp")

    spark.sql(
      """
        |create table spark_on_hive_exercise.tb_external_info(
        | name string,
        | height double
        |) row format delimited
        | fields terminated by ','
        | location 'hdfs://ns1/spark-sql/emp_external_info'
      """.stripMargin)

    spark.sqlContext.cacheTable("spark_on_hive_exercise.tb_external_info")

    //    spark.sql("select * from spark_on_hive_exercise.tb_emp").show
    //    println("\n_________________\n")
    //    spark.sql("select * from spark_on_hive_exercise.tb_external_info").show


    //    内连接查询
    //方式1：结果输出到控制台
    //    spark.sql(
    //      """
    //        |select
    //        | e.name `名字`,
    //        | e.age `年龄`,
    //        | if(e.isMarried,'已婚','未婚') `婚否`,
    //        | i.height `身高`
    //        |from spark_on_hive_exercise.tb_emp e,spark_on_hive_exercise.tb_external_info i
    //        |where e.name=i.name
    //      """.stripMargin)
    //        .show


    val df = spark.sql(
      """
        |select
        | e.name ,
        | e.age ,
        | if(e.isMarried,'已婚','未婚')  `isMarried`,
        | i.height
        |from spark_on_hive_exercise.tb_emp e,spark_on_hive_exercise.tb_external_info i
        |where e.name=i.name
      """.stripMargin)

    //方式2：结果落地到hive表
     df.write.saveAsTable("spark_on_hive_exercise.tb_final_result")

    println("\n______________________________________________\n")

    //        hdfs特定目录下
    //
    //df.write.mode(SaveMode.Overwrite).json("hdfs://ns1/spark-sql2/output")

    //    Stop
    //stop
    spark.stop()
  }
}
