package com.liu.apitest.state;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyedState只对当前的key有效（指来一个处理一个）
 * 前面出现过sensor1 2次-2，之后sensor6 1次-1，然后sensor1 1次-3
 */
public class StateTest2_KeyedState {

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

        DataStream<Integer> resultStream = dataStream.keyBy("id").map(new MyKeyCountMapper());

        resultStream.print();

        env.execute();
    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        // 定义状态
        // 注意：getRuntimeContext()必须等到生命周期open之后才能调用
        private ValueState<Integer> keyCountState;

        // 其他类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("key-count", Integer.class, 1));

            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<String>("my-list", String.class));

            myMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));

            // 定义出聚合的过程，告诉应该怎么聚合
//            myReducingState = getRuntimeContext().getReducingState(
//                    new ReducingStateDescriptor<SensorReading>());
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            // 获取状态
            Integer count = keyCountState.value();
            count += 1;
            keyCountState.update(count);

            // 其他api调用
//            Iterable<String> strings = myListState.get();
            for (String str : myListState.get()) {
                System.out.println(str);
            }
            myListState.add("hello");

            myMapState.get("1");
            myMapState.put("2", 12.5);
            myMapState.remove("2");

//            myReducingState.add();

//            myListState.clear();

            return count;
        }
    }
}
