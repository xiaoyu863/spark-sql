package readfrom

import java.util.Properties

import com.qf.util.SparkUtil

/**
  * Description：从RDBMS中读取数据到临时表DataFrame中演示<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object ReadDataFromRDBMSDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession(
      ReadDataFromRDBMSDemo.getClass.getSimpleName,
      "local[*]")

    //读取db之spark-core-study中的表tb_per_hour_province_top3中的数据
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "88888888")

    spark.read
      .jdbc("jdbc:mysql://NODE03:3306/spark-core-study?useUnicode=true&characterEncoding=utf-8",
        "tb_per_hour_province_top3", properties)
      .show(10000)

    //stop
    spark.stop()
  }
}
