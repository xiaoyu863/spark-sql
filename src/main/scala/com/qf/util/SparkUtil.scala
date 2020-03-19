package com.qf.util

import org.apache.spark.sql.SparkSession

/**
  * Description：工具方法<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月01日  
  *
  * @author 徐文波
  * @version : 1.0
  */
object SparkUtil {
  /**
    * 获得SparkSession的实例
    *
    * @param appName
    * @param master
    * @return
    */
  def getSparkSession(appName: String, master: String) =
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate
}
