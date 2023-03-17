package com.czl.submitter.spark.enums;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/7/25
 * Description:
 */
public enum DriverState {
    SUBMITTED,
    RUNNING,
    FINISHED,
    RELAUNCHING,
    UNKNOWN,
    KILLED,
    FAILED,
    ERROR,
    QUEUED,
    RETRYING,
    NOT_FOUND
}
