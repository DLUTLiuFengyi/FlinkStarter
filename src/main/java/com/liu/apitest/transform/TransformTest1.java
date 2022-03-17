package com.liu.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flatmap主要功能是将一行数据中的每个字段信息都拆分出来
 */
public class TransformTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取进来的只能是String
        DataStream<String> dataStream = env.readTextFile(
                "D:\\gitrepos\\FlinkStarter\\src\\main\\resources\\sensor.txt"
        );

        DataStream<String> flatMapStream = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                // s是一行数据
                String[] fields = s.split(",");
                for (String field : fields) {
                    // 将每个元素中的每个字段信息都单独输出
                    collector.collect(field);
                }
            }
        });

//        DataStream<String> filterStream = dataStream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return s.startsWith("sensor_1");
//            }
//        });

        flatMapStream.print("flatmap");
//        filterStream.print("filter");

        env.execute();
    }
}
