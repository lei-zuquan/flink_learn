package com.scala.core.util


import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:57 上午 2020/4/18
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

object MyEsUtil {


  val httpHosts = new util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("hadoop1",9200,"http"))
  httpHosts.add(new HttpHost("hadoop2",9200,"http"))
  httpHosts.add(new HttpHost("hadoop3",9200,"http"))


  def  getElasticSearchSink(indexName:String):  ElasticsearchSink[String]  ={
    val esFunc = new ElasticsearchSinkFunction[String] {
      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        println("试图保存："+element)
        val jsonObj: JSONObject = JSON.parseObject(element)
        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jsonObj)
        indexer.add(indexRequest)
        println("保存1条")
      }
    }

    val sinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, esFunc)

    //刷新前缓冲的最大动作量
    sinkBuilder.setBulkFlushMaxActions(10)


    sinkBuilder.build()
  }

}
