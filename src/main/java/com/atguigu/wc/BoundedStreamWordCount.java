package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * author:chenminl
 * date:2023/2/5 14:00
 * 描述：有界流处理
 **/
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流式的执行环境 并行数：当前电脑的cpu核数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        //转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //分组  【分组的时侯，把同一组数据分到同一个并行自任务上去，在同一个任务中，才能累加求和】
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        //打印输出
        sum.print();

        //启动执行
        env.execute();
    }
}

