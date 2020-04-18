package com.scala.core

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:46 上午 2020/4/15
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object C_02DataStreamWcApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    // 需要导入隐士转换
    val sumDstream = dataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    sumDstream.print()

    // 实时分析，最后得调用执行触发
    env.execute()
  }

}
