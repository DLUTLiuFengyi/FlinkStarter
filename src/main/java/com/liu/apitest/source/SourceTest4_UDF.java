package com.liu.apitest.source;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

/**
 * 关键在于在run函数中维护一个<id, temp>的哈希map
 */
public class SourceTest4_UDF {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 定义一个标志位用来控制数据的产生
        private boolean running = true;

        /**
         *
         * @param sourceContext source的上下文
         * @throws Exception
         */
        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();

            // 设置10个传感器的初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i=0; i<10; i++) {
                sensorTempMap.put("sensor_" + (i+1), 60 + random.nextGaussian()*20);
            }

            // 只要为true就不停地生成
            while (running) {
                // 实际要监视很多个传感器，会有多个sensorid
                // 挨个更新sensor温度值
                for (String sensorId : sensorTempMap.keySet()) {
                    // 在当前温度基础上随机波动
                    Double newtemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newtemp);
                    sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), newtemp));
                }
                // 控制输出频率
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
