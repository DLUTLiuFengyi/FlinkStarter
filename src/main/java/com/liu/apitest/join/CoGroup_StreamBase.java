package com.liu.apitest.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * https://blog.csdn.net/qq_37142346/article/details/90176504
 *
 * cogroup类似于全连接
 * 只要仍一边有数，都会输出
 *
 * 终端1：
 * 1 lj
 * 1 al
 * 2 af
 *
 * 终端2：
 * 2 ac
 * 1 ao
 * 2 14
 */
public class CoGroup_StreamBase {

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

        DataStream<Object> resultStream = input1.coGroup(input2)
                // 选择进行连接的key
                .where(new KeySelector<Tuple2<String, String>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, String>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .trigger(CountTrigger.of(1))
                .apply(new CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, Object>() {

                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> first,
                                        Iterable<Tuple2<String, String>> second, Collector<Object> out)
                            throws Exception {
                        StringBuffer buffer = new StringBuffer();
                        buffer.append("input stream 1: \n");
                        for (Tuple2<String, String> value : first) {
                            buffer.append(value.f0 + "=>" + value.f1 + "\n");
                        }
                        buffer.append("input stream 2: \n");
                        for (Tuple2<String, String> value : second) {
                            buffer.append(value.f0 + "=>" + value.f1 + "\n");
                        }
                        out.collect(buffer.toString());
                    }
                });

        resultStream.print();

        env.execute();
    }
}
