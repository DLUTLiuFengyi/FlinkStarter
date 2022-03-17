package com.liu.apitest.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 将两个数据流按照相同的key，且在其中一个流的时间范围内的数据进行join处理
 *
 * 通常用于把一定时间范围内相关的分组数据拉成一个宽表
 *
 * key1 == key2 && e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound
 */
public class Join_IntervalJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream1 = env.socketTextStream("localhost", 17777);
        DataStream<String> inputStream2 = env.socketTextStream("localhost", 17778);

        DataStream<Tuple2<String, String>> input1 = inputStream1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return Tuple2.of(value.split(",")[0], value.split(",")[1]);
            }
        });
        DataStream<Tuple2<String, String>> input2 = inputStream2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return Tuple2.of(value.split(",")[0], value.split(",")[1]);
            }
        });

        DataStream<Object> resultStream = input1.keyBy(new KeySelector<Tuple2<String, String>, Object>() {
            @Override
            public Object getKey(Tuple2<String, String> t) throws Exception {
                return t.f0;
            }
        }).intervalJoin(input2.keyBy(new KeySelector<Tuple2<String, String>, Object>() {
            @Override
            public Object getKey(Tuple2<String, String> t) throws Exception {
                return t.f0;
            }
        })).between(Time.seconds(-3), Time.seconds(3))
                .process(new ProcessJoinFunction<Tuple2<String, String>, Tuple2<String, String>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, String> left, Tuple2<String, String> right,
                                               Context ctx, Collector<Object> out) {
                        out.collect(left.f1 + " " + right.f1);
                    }
                });

        resultStream.print();

        env.execute();
    }
}
