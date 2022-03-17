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
 * 定时器功能
 */
public class ProcessTest1_KeyedProcessFunction {

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
                .process(new MyProcess());

        resultStream.print();

        env.execute();
    }

    /**
     * 本身是RichFunction，因此也可以有open等函数，以及getRuntimeContext等方法
     */
    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        private ValueState<Long> tsTimerState; // 这里暂时没啥用

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, Integer>.Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // 上下文
            ctx.timestamp();
            ctx.getCurrentKey();
            // output tag说明可以在这里进行分流的定义，侧输出
//            ctx.output();
            // 定时服务
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();

            // 传入要触发的时间戳
//            ctx.timerService().registerProcessingTimeTimer(10000L); // 从1970标准时间算起10000ms

            // 注册定时器，2秒后触发
            System.out.println(ctx.timerService().currentProcessingTime() + "注册定时器，2秒后触发");
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
//            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 20*1000));

            // 对当前时间戳做一个保存
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 2000L);
            // 取消定时器
//            ctx.timerService().deleteProcessingTimeTimer(10000L);
            // 可以设置多个定时器，按时间区分
        }

        /**
         * 定义闹钟响时要做的事
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时器触发");
//            ctx.getCurrentKey();
////            ctx.output();
//            ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }
}
