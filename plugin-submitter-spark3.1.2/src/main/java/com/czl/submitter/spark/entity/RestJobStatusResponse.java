package com.czl.submitter.spark.entity;

import com.czl.submitter.spark.enums.DriverState;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/7/25
 * Description: rest方式查询spark任务
 */
public class RestJobStatusResponse extends RestSparkResponse {

    private String workerHostPort;

    private String workerId;

    private DriverState driverState;

    public String getWorkerHostPort() {
        return workerHostPort;
    }

    public String getWorkerId() {
        return workerId;
    }

    public DriverState getDriverState() {
        return driverState;
    }
}
