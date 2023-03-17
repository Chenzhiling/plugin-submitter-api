package com.czl.submitter.spark.entity;

import com.czl.submitter.core.entity.QueryRequest;
import com.czl.submitter.core.enums.ExecutionMode;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/17
 * Description:
 */
public class SparkQueryRequest extends QueryRequest {


    private String yarnFile;


    public SparkQueryRequest(ExecutionMode executionMode,
                             String master,
                             String taskId,
                             String yarnFile) {
        super(executionMode, master, taskId);
        this.yarnFile = yarnFile;
    }

    public String getYarnFile() {
        return yarnFile;
    }
}
