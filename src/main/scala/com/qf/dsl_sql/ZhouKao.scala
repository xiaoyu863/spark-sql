package com.qf.dsl_sql

import com.qf.util.SparkUtil
import org.apache.spark.sql.DataFrame


object ZhouKao {
  val spark = SparkUtil.getSparkSession("local[2]", this.getClass.getSimpleName)

  import spark.implicits._
  import org.apache.spark.sql.functions._

  /**
    * 第一题：分类统计businessarea
    */
  private def countBusinessarea(origin_table: DataFrame) = {
    origin_table.select(
      explode($"regeocode.pois").as("pois")
    ).groupBy($"pois.businessarea".as("businessarea"))
      .agg(count("*").as("count"))
  }

  /**
    * 第二题：分类统计标签数量
    */
  private def count_markType(origin_table: DataFrame) = {
    origin_table.select(
      explode($"regeocode.pois").as("pois")
    ).select(
      explode(split($"pois.type", "\\|")).as("types")
    ).select(
      explode(split($"types", ";")).as("type")
    )
      .groupBy("type")
      .agg(count("*").as("countType"))
  }

  def main(args: Array[String]): Unit = {
    val rootDir = "C:\\Users\\Administrator\\Desktop\\机考题\\机考题\\"
    val origin_table = spark.read.json(rootDir + "json.json").filter($"status" === 1)
    //结构树
    origin_table.schema.printTreeString()
    //统计businessarea
    countBusinessarea(origin_table).show(20)
    //统计标签
    count_markType(origin_table).show(20)
  }
}
