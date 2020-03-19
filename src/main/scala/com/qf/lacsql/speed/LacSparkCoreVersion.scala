package com.qf.lacsql.speed

import com.qf.util.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Description：RDD算子综合性的案例之基站<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月01日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object LacSparkCoreVersion {
  def main(args: Array[String]): Unit = {
    //开始计时
    val beginTime = System.currentTimeMillis()

    //SparkSession
    val sc: SparkContext = SparkUtil.getSparkSession(
      LacSparkCoreVersion.getClass.getSimpleName,
      "local[*]"
    ).sparkContext

    //    1，将各个用户途径基站在后台留下的日志信息文件装载进内存，据此构建rdd
    //    2，计算出各个用户途径各个基站停留的时长，进行时长降序排列，取出前两个即可，结果类型形如：RDD[基站id,(手机号码，停留的时长)]
    //18101056888,20180327180000,16030401EAFB68F1E3CDF819735E1C66,0
    val logRDD: RDD[(String, (String, Long))] = sc.textFile("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\mobilelocation\\log")
      .map(perline => {
        val arr = perline.split(",")
        val phoneNum = arr(0).trim
        val tmpTime = arr(1).trim.toLong
        val lacId = arr(2).trim
        val flg = arr(3).trim.toInt
        //0：离开，1：进入
        val time = if (flg == 1) -tmpTime else tmpTime
        //根据手机号和基站id进行分组，将其作为分组依据，若据此进行聚合，结果即为：用户在各个基站停留的时长
        ((phoneNum, lacId), time)
      }).reduceByKey(_ + _)
      .groupBy(_._1._1)
      //.mapValues(_.toList.sortBy(_._2).reverse.take(2))
      .mapValues(_.toList.sortWith(_._2 > _._2).take(2))
      .map(_._2)
      .flatMap(x => x)
      .map(perEle => (perEle._1._2, (perEle._1._1, perEle._2)))

    // (16030401EAFB68F1E3CDF819735E1C66,(18101056888,97500))
    // (16030401EAFB68F1E3CDF819735E1C66,(18688888888,87600))
    // (9F36407EAD8829FC166F14DDE7970F68,(18101056888,54000))
    // (9F36407EAD8829FC166F14DDE7970F68,(18688888888,51200))

    //    3，将基站信息信息文件装载进内存，据此构建rdd[基站id,(经度，维度)]
    //16030401EAFB68F1E3CDF819735E1C66,116.296302,40.032296,6
    val lacRDD: RDD[(String, (String, String))] = sc.textFile("file:///C:\\Users\\Administrator\\IdeaProjects\\spark-sql-study\\data\\mobilelocation\\lac_info.txt")
      .map(perline => {
        val arr = perline.split(",")
        val lacId = arr(0).trim
        val jd = arr(1).trim
        val wd = arr(2).trim
        (lacId, (jd, wd))
      })

    //lacRDD.foreach(println)

    //(9F36407EAD8829FC166F14DDE7970F68,(116.304864,40.050645))
    //(16030401EAFB68F1E3CDF819735E1C66,(116.296302,40.032296))
    //(CC0710CC94ECC657A8561DE549D940E0,(116.303955,40.041935))

    //4，将步骤2,3进行内连接，即可求出：用户的公司地址和家庭住址
    //5，输出结果即可
    logRDD.join(lacRDD)
      .map(perEle => (perEle._2._1._1, perEle._2._2, perEle._2._1._2))
      .foreach(perEle => println(s"手机号码：${perEle._1},经纬度：${perEle._2},停留时长：${perEle._3}"))

    //手机号码：18101056888, 经纬度：(116.304864,40.050645), 停留时长：54000
    //手机号码：18101056888, 经纬度：(116.296302,40.032296), 停留时长：97500
    //手机号码：18688888888, 经纬度：(116.304864,40.050645), 停留时长：51200
    //手机号码：18688888888, 经纬度：(116.296302,40.032296), 停留时长：87600

    //释放
    sc.stop()

    //结束计时
    val endTime = System.currentTimeMillis()
    println(s"→ spark Core运行花费的时间是：${endTime - beginTime}ms")

    // → spark Core运行花费的时间是：9239ms ← 第一次
    //→ spark Core运行花费的时间是：8989ms ← 第二次
    // → spark Core运行花费的时间是：8248ms ← 第三次
  }
}
