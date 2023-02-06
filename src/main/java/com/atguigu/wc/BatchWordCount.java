package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * author:chenminl
 * date:2023/2/5 13:47
 * 描述：批处理统计
 **/
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        //将每行数据进行分词，转换成二元组类型 【returns 指定二元组元素的类型】
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            //将一行文本进行分词
            String[] words = line.split(" ");
            //将每个单词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //已Types.TUPLE第一个元素作为索引的key，进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        //分组内进行聚合统计Types.TUPLE二个元素的值，进行累加
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        //打印输出
        sum.print();
    }
}

