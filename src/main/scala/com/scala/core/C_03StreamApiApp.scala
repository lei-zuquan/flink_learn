package com.scala.core

import com.alibaba.fastjson.JSON
import com.scala.core.bean.Startuplog
import com.scala.core.util.MyKafkaUtil
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:17 上午 2020/4/17
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object C_03StreamApiApp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    environment.setParallelism(2)

    val kafkaConsumer = MyKafkaUtil.getConsumer("GMALL_STARTUP")

    /*
    {"area":"bei jing","uid":"226","os":"android","ch":"baidu","appid":"gmall1205","mid":"mid_336","type":"startup","vs":"1.1.3","ts":1559801977906}
     */
    val dstream: DataStream[String] = environment.addSource(kafkaConsumer)


    // DataStream这种流是无状态
    val startuplogDstream: DataStream[Startuplog] = dstream.map { jsonStr => JSON.parseObject(jsonStr, classOf[Startuplog]) }
    dstream.print()

//    /**
//     * 统计各个渠道的累计个数
//     */
//    // KeyedStream这种流是有状态的
//    val chKeyedStream: KeyedStream[(String, Int), Tuple] = startuplogDstream.map(startuplog => (startuplog.ch, 1)).keyBy(0)
//
//    val chSumDstream: DataStream[(String, Int)] = chKeyedStream.reduce((ch1, ch2) => (ch1._1, ch1._2 + ch2._2))
//    //dstream.print()
//    chSumDstream.print("开头：").setParallelism(1)

//G

    environment.execute()
  }

}
