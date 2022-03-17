package com.liu.apitest.transform;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest6_Partition {

    public static void main(String[] args) throws Exception {
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

        inputStream.print("input");

        // 1. shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");

        // 2. keyBy
        dataStream.keyBy("id").print("keyBy");

        // 把所有数据全部发送的下游的一个分区内
        dataStream.global().print("global");

        env.execute();
    }
}
