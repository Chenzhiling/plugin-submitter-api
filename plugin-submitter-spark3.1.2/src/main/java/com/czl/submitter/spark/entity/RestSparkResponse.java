package com.czl.submitter.spark.entity;

import com.czl.submitter.spark.enums.RestSparkAction;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/7/25
 * Description:
 */
public class RestSparkResponse {

    RestSparkResponse() {}

    private RestSparkAction action;

    private String message;

    private String serverSparkVersion;

    private String submissionId;

    private Boolean success;

    public RestSparkAction getAction() {
        return action;
    }

    public String getMessage() {
        return message;
    }

    public String getServerSparkVersion() {
        return serverSparkVersion;
    }

    public String getSubmissionId() {
        return submissionId;
    }

    public Boolean getSuccess() {
        return success;
    }

}
