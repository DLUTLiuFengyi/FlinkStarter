package com.liu.apitest.state;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_FaultTolerance {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置状态后端
        env.setStateBackend(new MemoryStateBackend());

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
    }
}
