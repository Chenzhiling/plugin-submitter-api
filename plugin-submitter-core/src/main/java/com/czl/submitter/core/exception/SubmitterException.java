package com.czl.submitter.core.exception;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/15
 * Description:
 */
public class SubmitterException extends RuntimeException {

    public SubmitterException(Throwable cause) {
        super(cause);
    }

    public SubmitterException(String errMsg) {
        super(errMsg);
    }

    public SubmitterException(String errMsg, Throwable cause) {
        super(errMsg, cause);
    }
}
