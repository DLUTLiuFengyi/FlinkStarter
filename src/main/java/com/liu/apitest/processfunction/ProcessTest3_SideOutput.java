package com.liu.apitest.processfunction;

import com.liu.apitest.entry.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 判断温度，大于30度，高温流输出到主流，低温流输出到侧输出流
 */
public class ProcessTest3_SideOutput {

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

        // 定义一个outputTag，用来表示侧输出低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {

        };

        // 直接process，自定义输出流
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx,
                                       Collector<SensorReading> out) throws Exception {
                // 判断温度，大于30度，高温流输出到主流，低温流输出到侧输出流
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(lowTempTag, value);
                }
            }
        });

        highTempStream.print("high-temp");
        // 从高温流中开出一条低温流
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }
}
