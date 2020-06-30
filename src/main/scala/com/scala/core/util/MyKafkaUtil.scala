package com.scala.core.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.api.scala._

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:18 上午 2020/4/17
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
<!-- https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.11 -->
<dependency>
<groupId>org.apache.flink</groupId>
<artifactId>flink-connector-kafka-0.11_2.11</artifactId>
<version>1.7.0</version>
</dependency>
*/
object MyKafkaUtil {
  val prop = new Properties()

  val kafka_broker_list = "node-01:9092,node-02:9092,node-03:9092"
  prop.setProperty("bootstrap.servers", kafka_broker_list)
  prop.setProperty("group.id", "gmall")

  def getConsumer(topic:String ):FlinkKafkaConsumer011[String]= {
    val myKafkaConsumer:FlinkKafkaConsumer011[String] =
      new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)

    myKafkaConsumer
  }

  def getKafkaSink(topic: String): FlinkKafkaProducer011[String] = {
    val kafkaSink = new FlinkKafkaProducer011[String](kafka_broker_list, topic, new SimpleStringSchema())
    kafkaSink
  }

}
