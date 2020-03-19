package com.qf.testz5

import com.alibaba.fastjson.{JSON, JSONObject}
import com.qf.lacsql.LacDemo
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 *
 * 考试题 三种方法
 * dsl
 * sql
 * spark core
 *
 */
object jiubao {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(LacDemo.getClass.getSimpleName)
      .getOrCreate()
    val input = args(0)
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val df = spark.read.json(input)
      .createOrReplaceTempView("tab1")

    spark.sql(
      """
        |select
        |tab2.area,
        |count(*) from
        |(select
        |explode(regeocode.pois.businessarea) area
        |from
        |tab1
        |) tab2
        |group by tab2.area
        |""".stripMargin)
      .show()

      spark.sql(
        """
          |select
          |_type2,
          |count(*)
          |from
          |(select
          |explode(split(_type,';')) _type2
          |from
          |(select
          |explode(regeocode.pois.type) _type
          |from
          |tab1
          |) tab2
          |)
          |group by
          |_type2
          |""".stripMargin)
        .show()

    //status
    sc.textFile(input).filter(
      x => {
        val jsonObject = JSON.parseObject(x)
        if (jsonObject.getString("status").equals("1"))
          true
        else
          false
      }
    ).map(
      x => {
        var list = List[(String, String)]()
        val jsonObject = JSON.parseObject(x)
        val json2 = jsonObject.getJSONObject("regeocode").getJSONArray("pois")
        var i = 0
        while (i < json2.size()) {
          val json3 = json2.getJSONObject(i)
          list :+= (json3.getString("businessarea"), json3.getString("type"))
          i = i + 1
        }
        list
      }
    ).flatMap(x => x)
      .groupBy(_._1)
      .mapValues(_.size)
      .foreach(println)


    sc.textFile(input).filter(
      x => {
        val jsonObject = JSON.parseObject(x)
        if (jsonObject.getString("status").equals("1"))
          true
        else
          false
      }
    ).map(
      x => {
        var list = List[(String, String)]()
        val jsonObject = JSON.parseObject(x)
        val json2 = jsonObject.getJSONObject("regeocode").getJSONArray("pois")
        var i = 0
        while (i < json2.size()) {
          val json3 = json2.getJSONObject(i)
          list :+= (json3.getString("businessarea"), json3.getString("type"))
          i = i + 1
        }
        list
      }
    ).flatMap(x => x)
      .map(x=>x._2.split(";"))
      .flatMap(x=>x)
      .groupBy(x=>x)
      .mapValues(_.size)
      .foreach(println)

    spark.close()

  }
}
