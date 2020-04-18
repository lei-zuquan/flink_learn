package com.java.core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:01 下午 2020/4/14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C01_Java_WordCount {
    public static void main(String[] args) throws Exception {
        //构造执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取文件
        String input = "input_data/hello.txt"; //"file:///d:/temp/hello.txt"
        DataStream<String> text = env.readTextFile(input);

//        text.flatMap(new Tokenizer() {
//            @Override
//            public boolean incrementToken() throws IOException {
//                return false;
//            }
//        })
//        //打印
//        wordCount.print();
    }

}


class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>>{

    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
        for (String word: line.split(" ")){
            out.collect(new Tuple2<>(word,1));
        }
    }
}

