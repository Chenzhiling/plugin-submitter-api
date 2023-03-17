package com.czl.submitter.flink.entity;

import com.czl.submitter.core.entity.SubmitResponse;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/16
 * Description:
 */
public class FlinkSubmitResponse extends SubmitResponse {

    private String clusterId;

    public String getClusterId() {
        return clusterId;
    }

    public FlinkSubmitResponse(String clusterId,String taskId) {
        super(taskId);
        this.clusterId = clusterId;
    }
}
