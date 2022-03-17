package com.liu.apitest.state;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * case: 检测温度值，如果某个传感器当前温度与上一次温度的差值超过阈值，则报警
 *
 * valuestate会与key绑定，上次的数据被不同key的数据“冲刷”后，下次还会正常显示报警
 * 实际上keyBy分区之后数据之间互不影响
 *
 * out.collect(xxx) 代表生成并往命令行输出xxx内容
 */
public class StateTest3_KeyedStateApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

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

        Double warningThreshold = 10.0;

        // 定义一个flatmap操作，检测温度跳变，输出报警
        DataStream<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(warningThreshold));

        resultStream.print();

        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        // 当前温度跳变的阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Double.class)
            );
        }

        @Override
        public void flatMap(SensorReading value,
                            Collector<Tuple3<String, Double, Double>> out)
                throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();

            // 如果状态不为null，那么就判断两次温度差值
            if (lastTemp != null) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
                }
            }

            // 更新状态
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
