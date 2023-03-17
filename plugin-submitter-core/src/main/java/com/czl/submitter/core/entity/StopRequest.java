package com.czl.submitter.core.entity;

import com.czl.submitter.core.enums.ExecutionMode;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/15
 * Description:
 */
public class StopRequest {

    private ExecutionMode executionMode;

    private String master;

    private String taskId;

    public ExecutionMode getExecutionMode() {
        return executionMode;
    }

    public String getMaster() {
        return master;
    }

    public String getTaskId() {
        return taskId;
    }

    public StopRequest(ExecutionMode executionMode, String master, String taskId) {
        this.executionMode = executionMode;
        this.master = master;
        this.taskId = taskId;
    }
}
