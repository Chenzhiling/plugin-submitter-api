package com.czl.submitter.core.enums;

import java.io.Serializable;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/5/26
 * Description: support mode
 */
public enum ExecutionMode implements Serializable {


    STANDALONE(1, "standalone"),

    FLINK_YARN_SESSION(2, "yarn-session"),

    FLINK_YARN_PER_JOB(3,"yarn-per-job"),

    SPARK_YARN(4, "spark_yarn");

    private final Integer mode;

    private final String name;

    ExecutionMode(Integer mode, String name) {
        this.mode = mode;
        this.name = name;
    }


    public int getMode() {
        return mode;
    }

    public String getName() {
        return name;
    }

    public static ExecutionMode of(Integer value) {
        for (ExecutionMode executionMode : values()) {
            if (executionMode.mode.equals(value)) {
                return executionMode;
            }
        }
        return null;
    }
}
