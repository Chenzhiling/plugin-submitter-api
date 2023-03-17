package com.czl.submitter.core.entity;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/15
 * Description:
 */
public class SubmitResponse {

    /**
     * Spark submissionId
     * Flink appId
     */
    private String taskId;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public SubmitResponse(String taskId) {
        this.taskId = taskId;
    }
}
