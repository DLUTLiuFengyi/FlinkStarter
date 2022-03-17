package com.liu.apitest.processfunction;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * case: 检测温度值，如果某个传感器在10秒内温度持续上升的话，则报警
 * 如果用状态编程的思路
 * 10秒的需求如果开滚动窗口的话，不太对
 * 因为不知道数据什么时候来，什么时候判断
 * 比如数据先上升，后下降，然后又上升，在单个窗口内无法判断是否满足连续10秒上升的情况
 * 如果滑动窗口的话，要准确只能开1秒的窗口，但效率非常低
 * 因此不能定死窗口长度
 * 最好方式是从第一个数据来时，注册一个10秒后触发的定时器，然后收集10秒内的数据
 * 定时器触发时，对这10秒内的数据进行判断
 *
 * 和之前的case类似，key id各自看各自的，互不干涉
 */
public class ProcessTest2_KeyedProcessApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("localhost", 17777);

        // 转换
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        // 测试keyprocessfunc，先分组，然后自定义process
        DataStream<Integer> resultStream = dataStream.keyBy("id")
                .process(new ProcessTest1_KeyedProcessFunction.MyProcess());

        resultStream.print();

        env.execute();
    }

    /**
     * 输出是异常sensor的id
     */
    public static class TempConsIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private Integer interval; // 当前统计的时间间隔

        public TempConsIncreaseWarning(Integer interval) {
            this.interval = interval;
        }

        // 定义状态
        private ValueState<Double> lastTempState; // 上一次温度值
        private ValueState<Long> timerTsState; // 定时器时间戳

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE)
            );
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer-ts", Long.class)
            );
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 如果温度上升并且没有定时器，注册10秒后的定时器，开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                // 计算出定时器触发时的时间戳 = 当前时间戳 + interval 秒
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                // 注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            } else if (value.getTemperature() < lastTemp && timerTs != null) {
                // 如果温度下降，删除定时器（这一步是关键思路）
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                // 定时器状态也记得清空
                timerTsState.clear();
            }

            // 更新温度差
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发，输出报警信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0)
                    + "温度值连续" + interval + "s上升");
            timerTsState.clear();
        }
    }
}
