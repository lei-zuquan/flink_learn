package com.scala.core

import com.alibaba.fastjson.JSON
import com.scala.core.bean.StartUpLog
import com.scala.core.util.{MyKafkaUtil, MyRedisUtil}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.redis.RedisSink


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
    {"area":"bei jing","uid":"226","os":"android","ch":"huawei","appid":"gmall1205","mid":"mid_336","type":"startup","vs":"1.1.3","ts":1559801977906}
    {"area":"shen zhen","uid":"226","os":"ios","ch":"apple","appstore":"gmall1205","mid":"mid_336","type":"startup","vs":"1.1.3","ts":1559801977906}

     */
    val dstream: DataStream[String] = environment.addSource(kafkaConsumer)


    // DataStream这种流是无状态
    val startuplogDstream: DataStream[StartUpLog] = dstream.map { jsonStr => JSON.parseObject(jsonStr, classOf[StartUpLog]) }
    startuplogDstream.print()

//    /**
//     * 统计各个渠道的累计个数
//     */
//    // KeyedStream这种流是有状态的
//    val chKeyedStream: KeyedStream[(String, Int), Tuple] = startuplogDstream.map(startuplog => (startuplog.ch, 1)).keyBy(0)
//
//    val chSumDstream: DataStream[(String, Int)] = chKeyedStream.reduce((ch1, ch2) => (ch1._1, ch1._2 + ch2._2))
//    chSumDstream.print("开头：").setParallelism(1)

    // 将appstore与其他渠道拆分拆分出来  成为两个独立的流
    val splitStream: SplitStream[StartUpLog] = startuplogDstream.split { startUplog =>
      var flags:List[String] =  null
      if ("appstore" == startUplog.ch) {
        flags = List("apple", "usa")
      } else if ("huawei" == startUplog){
        flags = List("android", "china")
      } else {
        flags = List("android", "other" )
      }
      flags
    }
    val appleStream: DataStream[StartUpLog] = splitStream.select("apple", "china")
    appleStream.print("apple:").setParallelism(1)
    val otherStream: DataStream[StartUpLog] = splitStream.select("other")
    otherStream.print("other:").setParallelism(1)
    

    // 合并流，方式一：connect合并流前后两者数据类型可不一致，后续通过coMap等进行数据的统一
    // connect一次只能合并两个流
//    val connStream: ConnectedStreams[StartUpLog, StartUpLog] = appleStream.connect(otherStream)
//    val allStream: DataStream[String] = connStream.map(
//      (startupLog1: StartUpLog) => startupLog1.ch,
//      (startupLog2: StartUpLog) => startupLog2.ch
//    )
//    allStream.print("All")

    // 合并流，方式二：使用union合并的前提上两个流的数据类型是一致的
    // union一次可以合并多个流
    val unionStream: DataStream[StartUpLog] = appleStream.union(otherStream)
    unionStream.print("Union")

    // 将合并流写入到KAFKA中
    val kafkaSink: FlinkKafkaProducer011[String] = MyKafkaUtil.getKafkaSink("topic_apple")
    val mapDataStream: DataStream[String] = unionStream.map(startUpLog => startUpLog.ch)
    //mapDataStream.addSink(kafkaSink)


    /**
     * 统计各个渠道的累计个数
     */
    // KeyedStream这种流是有状态的
    val chKeyedStream: KeyedStream[(String, Int), Tuple] = startuplogDstream.map(startuplog => (startuplog.ch, 1)).keyBy(0)

    val chSumDstream: DataStream[(String, Int)] = chKeyedStream.reduce((ch1, ch2) => (ch1._1, ch1._2 + ch2._2))
    chSumDstream.print("开头：").setParallelism(1)

    /*
    centos redis: redis-cli
    hset channal_sum xiaomi 100
    hset channal_sum huawei 100
    keys *
    hgetall channal_sum
     */
    // 把结果存入redis   hset  key:channel_sum   field:  channel   value:  count
    val redisSink: RedisSink[(String, String)] = MyRedisUtil.getRedisSink()
    //chSumDstream.addSink(redisSink)


    environment.execute()
  }

}
