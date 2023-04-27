package com.czl.submitter;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/11/21
 * Description:
 */
public class FlinkTaskBatch {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements(args);
        source.print().setParallelism(1);
        env.execute("flink-task");
    }
}
