package com.liu.apitest.state;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class StateTest1_OperatorState {

    public static void main(String[] args) throws Exception {
        org.apache.flink.streaming.api.environment.StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        DataStream<Integer> resultStream = dataStream.map(new MyCountMapper());

        // 如果并行度是1，则计算出所有元组个数，如果并行读为n，则n个线程分别计算其个数
        resultStream.print();

        env.execute();
    }

    // 自定义MapFunction
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>,
            ListCheckpointed<Integer> {

        // 定义一个本地变量，作为算子状态
        private Integer count = 0;
        // 本地变量在内存中，如果发送故障，则数据会丢
        // 因此想要容错需要存盘
        // 因此有ListCheckpointed<Integer>以及snapshotState、restoreState机制

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count += 1;
            return count;
        }

        /**
         * 对状态做快照，返回一个List<Integer>类型
         */
        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        /**
         * 发生故障后，根据快照进行恢复
         */
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
