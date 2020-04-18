package com.scala.core

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.{AggregateDataSet, DataSet}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:56 下午 2020/4/14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object C_01WordCount {
  def main(args: Array[String]): Unit = {
    // 核心思路：1/env   2/source    3/transform   4/sink

    // flink提供了一个工具类，读取args输入参数，类似key-value
    // Program arguments : --input input_data/hello.txt --output out_data/hello_out.txt
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val inputPath: String = tool.get("input")
    val outputPath: String = tool.get("output")

    //构造执行环境，处理离线计算；处理实时计算是另外一个
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //读取文件
    val input = "input_data/hello.txt" //"file:///d:/temp/hello.txt"
    val ds: DataSet[String] = env.readTextFile(input)
    // 其中flatMap 和Map 中  需要引入隐式转换
    import org.apache.flink.api.scala.createTypeInformation
    //经过groupby进行分组，sum进行聚合
    val aggSet: AggregateDataSet[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    // 打印
    //aggSet.print()
    aggSet.writeAsCsv(outputPath).setParallelism(1) // flink所有算子都可以单独设置并行度

    env.execute()

  }
}
