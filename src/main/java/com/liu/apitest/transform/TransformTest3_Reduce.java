package com.liu.apitest.transform;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * reduce是比max更加一般化的聚合操作
 *
 * keyBy按照哈希函数进行分区，相同key的都分到一个分区上
 */
public class TransformTest3_Reduce {

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
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // reduce聚合，取最大的温度值，以及当前最新的时间戳
        DataStream<SensorReading> reducedStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            // 参数：来一个t1，处理一个t1
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return new SensorReading(sensorReading.getId(), t1.getTimestamp(),
                        Math.max(sensorReading.getTemperature(), t1.getTemperature()));
            }
        });
//        keyedStream.reduce((curState, newData) -> {
//           return new SensorReading(curState.getId(), , newData.getTimestamp(),
//                   Math.max(curState.getTemperature(), newData.getTemperature()));
//        });

        reducedStream.print();

        env.execute();
    }
}
