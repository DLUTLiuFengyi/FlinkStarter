package com.liu.apitest.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import javax.annotation.Nullable;

/**
 * join的是每一个窗口中的数据流
 * 将一个窗口中相同key的数据按照inner join的方式进行连接
 * 然后在apply方法中实现JoinFunction或者FlatJoinFunction方法来处理并且发送到下游
 *
 * 终端1：
 * 1 hello
 * 2 world
 * 3 shinelon
 * 5 lllll
 * 4 sssss
 *
 * 终端2：
 * 1 hello
 * 2 limig
 * 3 nihao
 * 4 oooo
 *
 * 结果：
 * 4> hello hello
 * 2> limig world
 * 3> nihao shinelon
 * 1> oooo sssss
 *
 */
public class Join_TumblingWindowJoin {

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

        // event time必须设置wm，以保证有序性
        input1 = input1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, String>>() {
            private long max = 2000;
            private long currentTime;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTime - max);
            }

            @Override
            public long extractTimestamp(Tuple2<String, String> element, long previousElementTimestamp) {
                long timestamp = previousElementTimestamp;
                currentTime = Math.max(timestamp, currentTime);
                return currentTime;
            }
        });
        input2 = input2.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, String>>() {
            private long max = 5000;
            private long currentTime;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTime - max);
            }

            @Override
            public long extractTimestamp(Tuple2<String, String> element, long previousElementTimestamp) {
                long timestamp = previousElementTimestamp;
                currentTime = Math.max(timestamp, currentTime);
                return currentTime;
            }
        });

        DataStream<Object> resultStream = input1.join(input2)
                .where(new KeySelector<Tuple2<String, String>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, String> t) throws Exception {
                        return t.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, String>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, String> t) throws Exception {
                        return t.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(CountTrigger.of(1))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Object>() {
                    @Override
                    public Object join(Tuple2<String, String> tuple1, Tuple2<String, String> tuple2) throws Exception {
                        // 这里才是谓词判断的关键
                        if (tuple1.f0.equals(tuple2.f0)) {
                            return tuple1.f1 + " " + tuple2.f1;
                        }
                        return null;
                    }
                });

        resultStream.print();

        env.execute();
    }
}
