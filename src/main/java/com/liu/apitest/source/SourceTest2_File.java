package com.liu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取进来的只能是String
        DataStream<String> dataStream = env.readTextFile("D:\\gitrepos\\FlinkStarter\\src\\main\\resources\\sensor.txt");

        dataStream.print();

        env.execute();
    }
}
