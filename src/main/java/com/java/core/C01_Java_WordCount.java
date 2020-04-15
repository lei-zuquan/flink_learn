package com.java.core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
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
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件
        //String input = "input_data/hello.txt"; //"file:///d:/temp/hello.txt"
        //DataSource<String> ds = env.readTextFile(input);
        // 其中flatMap 和Map 中  需要引入隐式转换
        //import org.apache.flink.api.java.createTypeInformation
        //经过groupby进行分组，sum进行聚合
        /** AggregateOperator<Tuple2> sum = ds.flatMap(new FlatMapFunction<String, Tuple2>() {
            @Override
            public void flatMap(String line, Collector<Tuple2> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2(word, 1));
                }

            }
        }).groupBy(0).sum(1);

        // 打印
        sum.print();  */

        //数据源
        DataSet<String> text = env.fromElements("I have a good friend. She is a very nice person. The first time I saw her at school, she smiled at me and I like her a lot. When we do homework in a team, we become friends. As we share the same interest, we are close. Now we often hang out for fun and we cherish our friendship so much.");

        DataSet<Tuple2<String,Integer>> wordCount = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        //打印
        wordCount.print();
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

