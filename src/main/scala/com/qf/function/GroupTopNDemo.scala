package com.qf.function

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Description：分组求topN函数演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月07日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object GroupTopNDemo {
  def main(args: Array[String]): Unit = {
    //需求：根据分数文件，求出各个班上考分的前三名学生的信息。
    //SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(GroupTopNDemo.getClass.getSimpleName)
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd: RDD[Row] = sc
      .textFile("file:///F:\\intellij-workplace\\spark-sql\\data\\function\\scores.txt")
      .map(perLine => {
        val arr = perLine.split("\\s+")
        Row(arr(0).trim, arr(1).trim,arr(2).trim.toInt)
      })

    val structType = StructType(Seq(
      StructField("name", StringType, false),
      StructField("className", StringType, false),
      StructField("score", IntegerType, false)
    ))

    spark.createDataFrame(rdd, structType)
      .createOrReplaceTempView("tb_score")

    val bcLevel = sc.broadcast[Int](3)

    //方式1：row_number
        spark.sql(
          s"""
             |select
             |*
             |from(
             |   select
             |    *,
             |    row_number() over(partition by className order by score desc ) level
             |   from tb_score
             |)t
             |where level <=${bcLevel.value}
             |
          """.stripMargin)
          .show


    println("\n___________________________________\n")


    //方式2：dense_rank
    //特点：分数相同排名也相同，后续的名次依次递增，不会跳跃。
    //    |className|score|level|
    //    +---------+-----+-----+
    //    |   class2|   96|    1|
    //      |   class2|   88|    2|
    //      |   class2|   88|    2|
    //      |   class2|   87|    3|

    //    spark.sql(
    //      s"""
    //         |select
    //         |*
    //         |from(
    //         |   select
    //         |    *,
    //         |    dense_rank() over(partition by className order by score desc ) level
    //         |   from tb_score
    //         |)t
    //         |where level <=${bcLevel.value}
    //         |
    //      """.stripMargin)
    //      .show


   // println("\n___________________________________\n")

    //方式3： rank
    //特点：分数相同排名也相同，后续的名次会跳跃，具体跳跃重复的名词数。如：
    //    |className|score|level|
    //    +---------+-----+-----+
    //    |   class2|   96|    1|
    //      |   class2|   88|    2|
    //      |   class2|   88|    2|
    //      |   class2|   87|    4|

//    spark.sql(
//      s"""
//         |select
//         |*
//         |from(
//         |   select
//         |    *,
//         |    rank() over(partition by className order by score desc ) level
//         |   from tb_score
//         |)t
//         |where level <=${bcLevel.value}
//         |
//      """.stripMargin)
//      .show


    //stop
    spark.stop
  }
}
