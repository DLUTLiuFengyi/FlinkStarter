package com.liu.apitest.transform;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest5_RichFunction {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取进来的只能是String
        DataStream<String> inputStream = env.readTextFile(
                "D:\\gitrepos\\FlinkStarter\\src\\main\\resources\\sensor.txt"
        );

        // 转换
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyRichMapper());
    }

    public static class MyMapper implements MapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(), sensorReading.getId().length());
        }
    }

    // RichFunction不是一个接口而是一个抽象类
    public static class MyRichMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        // 初始化工作，一般先定义状态，或者建立数据库连接
        // 这样只需建立一次连接
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
//            getRuntimeContext().getState();
            return new Tuple2<>(sensorReading.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
