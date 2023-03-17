package com.czl.submitter.spark.entity;

import com.czl.submitter.core.entity.StopRequest;
import com.czl.submitter.core.enums.ExecutionMode;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/17
 * Description:
 */
public class SparkStopRequest extends StopRequest {

    private String yarnFile;

    public String getYarnFile() {
        return yarnFile;
    }

    public SparkStopRequest(ExecutionMode executionMode,
                            String master,
                            String taskId,
                            String yarnFile) {
        super(executionMode, master, taskId);
        this.yarnFile = yarnFile;
    }
}
