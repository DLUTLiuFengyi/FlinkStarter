package com.liu.apitest.window;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 每次输出是，每个窗口中各个id所对应的元素个数
 * 不关闭并行度1设置的话，会有输出的所有id为最后一个元素id的bug，聚合个数则正确
 */
public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        // 从文件读取进来的只能是String
//        DataStream<String> inputStream = env.readTextFile(
//                "D:\\gitrepos\\FlinkStarter\\src\\main\\resources\\sensor.txt"
//        );

        DataStream<String> inputStream = env.socketTextStream("localhost", 17777);

        // 转换
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 窗口测试
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.keyBy("id")
                // session window的话只能调底层
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)))
                // 传1个参数则是滚动窗口，传2个参数则是滑动窗口
                .timeWindow(Time.seconds(10))
                              // IN ACC OUT, ACC是累加器，中间聚合状态
                .aggregate(new MyAggregateFunc());

        resultStream.print();

        env.execute();
    }

    public static class MyAggregateFunc implements AggregateFunction
            <SensorReading, Integer, Tuple2<String, Integer>> {

        private String id = "";

        @Override
        public Integer createAccumulator() {
            // 从0开始
            return 0;
        }

        @Override
        public Integer add(SensorReading value, Integer accumulator) {
            id = value.getId();
            // 该如何累加
            return accumulator + 1;
        }

        @Override
        public Tuple2<String, Integer> getResult(Integer accumulator) {
            return new Tuple2<>(id, accumulator);
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            // 主要用于session window，分区合并
            // 但目前没有合并操作，数据都在各自分区
            return a + b;
        }
    }
}
