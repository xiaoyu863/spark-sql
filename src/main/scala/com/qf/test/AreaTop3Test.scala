package com.qf.test

import java.util
import java.util.Properties

import com.qf.function.MyBirthdayFunctionDemo.getMyInfo
import com.qf.function.udaf.MyAvg
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：按区域统计top3热门商品
  * 实现思路：
  *   1. 创建user_visit_action、product_info表到当前SparkSession并加载数据
  *   2. 从表user_visit_action中获取基础数据：用户点击流日志
  *     获取的字段有：city_id、click_product_id
  *     获取条件：日期范围为：startDate="2019-08-15"，endDate="2019-08-15"
  *              click_product_id（用户点击商品id字段）限定为不为空
  *   3. 获取mysql的city_info表的城市信息
  *   4. 将点击流日志和城市信息进行join，生成临时表tmp_click_product_basic，字段有：cityId, cityName, area, click_product_id
  *   5. 根据表tmp_click_product_basic，统计各区域商品点击次数并生成临时表tmp_area_product_click_count，字段有：area,product_id,click_count,city_infos
  *     city_infos的统计要求：
  *       因为每个区域有很多城市，需要将各个区域涉及到的城市信息拼接起来，比如华南区有广州和三亚，拼接后的city_infos为："4:三亚,3:广州"，其中数字4和3为city_id
  *       此时需要用到GroupConcatDistinctUDAF函数
  *   6. 将各区域商品点击次数临时表tmp_area_product_click_count的product_id字段去关联商品信息表(product_info)的product_id
  *     product_info表的extend_info字段值为json串，需要特殊处理："0"和"1"分别代表了自营和第三方商品
  *     需要用GetJsonObjectUDF函数是从json串中获取指定字段的值，如果product_status为0，返回值为"自营"，如果为1，返回值为"第三方"
  *     生成临时表tmp_area_fullprod_click_count的字段有：
  *     area,product_id,click_count,city_infos,product_name,product_status
  *   7. 将tmp_area_fullprod_click_count进行统计每个区域的top3热门商品（使用开窗函数进行子查询）
  *      统计过程中的外层查询需要增加area_level字段，即按照区域进行分级：
  *      区域有：华北、华东、华南、华中、西北、西南、东北
  *         A级：华北、华东
  *         B级：华南、华中
  *         C级：西北、西南
  *         D级：东北
  *      得到的结果字段有：area、area_level、product_id、city_infos、click_count、product_name、product_status
  *   8. 将结果保存到mysql的area_top3_product表中
  *
  */
object AreaTop3Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()

    // 指定获取数据的开始时间和结束时间
    val startDate = "2019-08-15"
    val endDate = "2019-08-15"


    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    /**
     *
     * 获取两张表中的数据
     *
     */
    sc.textFile("file:///F:\\intellij-workplace\\spark-sql\\data\\test\\product_info.txt")
      .map(perLine => {
        val arr = perLine.split(",")
        (arr(0).trim, arr(1).trim, arr(2).trim)
      }).toDF("product_id", "product_name", "extend_info")
      .createOrReplaceTempView("product_info")

    sc.textFile("file:///F:\\intellij-workplace\\spark-sql\\data\\test\\user_visit_action.txt")
      .map(perLine => {
        val arr = perLine.split(",")
        (arr(0).trim, arr(7).trim,arr(12).trim)
      }).filter(_._1.equals(startDate))
      .filter(_._2!="null")
      .map(x=>{(x._2,x._3)})
      .toDF("click_product_id", "city_id")
      .createOrReplaceTempView("user_visit_action")

    spark.read
      .jdbc(getProperties._2,
        "city_info", getProperties._1)
    .createOrReplaceTempView("city_info")

    spark.sql(
      """
        |
        |select ci.city_id city_id,
        |ci.city_name city_name,
        |ci.area area,
        |uva.click_product_id click_product_id
        |from
        |user_visit_action uva
        |left join
        |city_info ci
        |on uva.city_id=ci.city_id
        |""".stripMargin)
      .createOrReplaceTempView("tmp_click_product_basic")
    //字段有：area,product_id,click_count,city_infos

    spark.udf.register("GroupConcatDistinctUDAF", new GroupConcatDistinctUDAF)
    spark.udf.register[String, String ,String]("getStatus", (json: String,field:String) => new GetJsonObjectUDF().call(json,field))
    spark.sql(
      """
        |select
        |area,
        |click_product_id product_id,
        |count(*) click_count,
        |GroupConcatDistinctUDAF(concat(city_id,":",city_name)) city_infos
        |from
        |tmp_click_product_basic
        |group by
        |area,
        |click_product_id
        |""".stripMargin)
        .createOrReplaceTempView("tmp_area_product_click_count")
    //area,product_id,click_count,city_infos,product_name,product_status
    spark.sql(
      """
        |select
        |tap.area area,
        |tap.product_id product_id,
        |tap.click_count click_count,
        |tap.city_infos city_infos,
        |pi.product_name product_name,
        |(case getStatus(pi.extend_info,"product_status") when 0 then "自营"
        |else "第三方" end) product_status
        |from
        |tmp_area_product_click_count tap
        |left join
        |product_info pi
        |on tap.product_id=pi.product_id
        |""".stripMargin)
        .createOrReplaceTempView("tmp_area_fullprod_click_count")


    //|area|product_id|click_count|city_infos|product_name|product_status|
    spark.sql(
    """
        |select
        |tab.area area,
        |tab.area_level area_level,
        |tab.product_id product_id,
        |tab.city_infos city_infos,
        |tab.click_count  click_count,
        |tab.product_name product_name,
        |tab.product_status product_status
        |from
        |(select
        |area,
        |(case when area="华东" or area="华北" then "A级" when area="华南" or area="华中" then "B级"
        |when area="西北" or area="西南" then "C级" else "D级" end) area_level,
        |product_id,
        |click_count,
        |row_number() over (distribute by area sort by click_count desc) rank,
        |city_infos,
        |product_name,
        |product_status
        |from
        |tmp_area_fullprod_click_count) tab
        |where tab.rank<=3
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(getProperties._2,
        "area_top3_product", getProperties._1)

    spark.stop()
  }



  /**
    * 请求数据库的配置信息
    *
    * @return
    */
  def getProperties() = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    val url = "jdbc:mysql://xiaoyu1:3306/test?useUnicode=true&characterEncoding=utf8"
    (prop, url)
  }

}
