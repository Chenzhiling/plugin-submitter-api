package com.czl.submitter.spark.utils;

import com.czl.submitter.spark.enums.DriverState;

public final class RestSparkRequestException extends Exception {


    private DriverState state;

    public RestSparkRequestException(String message) {
        super(message);
    }

    public RestSparkRequestException(String message, DriverState driverState) {
        super(message);
        this.state = driverState;
    }

    public RestSparkRequestException(Throwable cause) {
        super(cause);
    }

    public DriverState getState() {
        return state;
    }
}
