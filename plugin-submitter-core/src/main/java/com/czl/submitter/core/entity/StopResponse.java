package com.czl.submitter.core.entity;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/16
 * Description:
 */
public class StopResponse {


    private Boolean isFinished;

    private String savePointPath;

    public StopResponse(Boolean isFinished, String savePointPath) {
        this.isFinished = isFinished;
        this.savePointPath = savePointPath;
    }

    public Boolean getFinished() {
        return isFinished;
    }

    public String getSavePointPath() {
        return savePointPath;
    }
}
