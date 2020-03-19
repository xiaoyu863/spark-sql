package com.qf.MySelf

import com.qf.lacsql.LacDemo
import com.qf.util.GetMyInfo
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EachTop2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(LacDemo.getClass.getSimpleName)
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

//    spark.udf.register[String, String ,Int]("getInfo", (year: String,which:Int) => getMyInfo(year,which))

    spark.udf.register[String, String ,Int]("getInfo", (website: String,which:Int) => getMyInfo(website,which))
    //spark.udf.register[String, String]("getSubject", (name: String) => getSubject(name))

    sc.textFile("file:///F:\\intellij-workplace\\spark-sql\\data\\subject\\access.txt")
      .map(perLine => {
        val arr = perLine.split(",")
        (arr(0).trim,arr(1).trim)
      }).toDF("date","ipInfo")
      .createOrReplaceTempView("websiteInfo")


    val df = spark.sql(
      """
        | select *
        | from
        | (
        | select
        | chart.subject s,
        | chart.model m,
        | chart.count c,
        | row_number() over(partition by subject order by chart.count desc) r
        | from
        | (select
        | getInfo(ipInfo,0) subject,
        | getInfo(ipInfo,1) model,
        | count(*) count
        | from
        | websiteInfo
        | group by
        | subject,model) chart
        | ) rchart
        | where rchart.r<=2
        |""".stripMargin
    )
    df.createOrReplaceTempView("df_info")

    df.show


//    spark.sql(
//
//        | select
//        | *
//        | from webInfo
//        |""".stripMargin
//    ).show

  }

  def getMyInfo(website:String,which:Int):String={
    GetMyInfo.getMyInfo(website,which)
  }


}
