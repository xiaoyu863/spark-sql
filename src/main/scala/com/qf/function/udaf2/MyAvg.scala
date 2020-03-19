package com.qf.function.udaf2

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


/**
  * Description：聚合函数之Aggregator方式自定义子类<br/>
  * Copyright (c) ，2019 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2019年11月08日  
  *
  * @author 徐文波
  * @version : 1.0
  */
class MyAvg extends Aggregator[Emp, AverageSalary, Double] {
  /**
    * 给用来存储结果的样例类AverageSalary进行初始化
    *
    * @return
    */
  override def zero: AverageSalary = AverageSalary(0.0, 0)

  /**
    * 每个分区中进行计算时
    * 每个Executor进程中所包含的所有线程，每个线程操作一个分区中的数据，计算时
    *
    * 每调用一次，统计当前分区中的一条记录
    *
    * @param b
    * @param a
    * @return
    */
  override def reduce(b: AverageSalary, a: Emp): AverageSalary = {
    b.totalSalary += a.salary
    b.totalCnt += 1
    b
  }

  /**
    * 进行全局合并
    *
    * @param b1 当前分区中汇总的结果（所有员工的薪资总和，员工总数）
    * @param b2 其他分区中汇总的结果（所有员工的薪资总和，员工总数）
    * @return
    */
  override def merge(b1: AverageSalary, b2: AverageSalary): AverageSalary = {
    b1.totalSalary += b2.totalSalary
    b1.totalCnt += b2.totalCnt

    b1
  }

  /**
    * 自定义函数返回最终的结果所回调的方法
    *
    * @param reduction
    * @return
    */
  override def finish(reduction: AverageSalary): Double = reduction.totalSalary / reduction.totalCnt

  /**
    *
    * 返回编码器的实例，对AverageSalary实例进行编码，为了提高实例在网络上传输的速度
    *
    * @return
    */
  override def bufferEncoder: Encoder[AverageSalary] = Encoders.product

  /**
    * 返回针对于自定义函数最终返回结果的编码器的实例
    *
    * @return
    */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
