package com.liu.apitest.transform;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 分组
        // 指定按照sensor的id进行分组
        // 写法1，数据类型指定为Tuple
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        // 写法2，数据类型可以是String
//        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());
//        KeyedStream<SensorReading, String> keyedStream2 = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合 相当于一个滚轮不停更新，来一个数据就进行处理、更新最大值
        // max的话，时间戳不会更新
//        DataStream<SensorReading> maxStream = keyedStream.max("temperature");
        // maxBy的话，时间戳会与最大值对应那条记录对应
        DataStream<SensorReading> maxStream = keyedStream.maxBy("temperature");

        maxStream.print();

        env.execute();
    }
}
