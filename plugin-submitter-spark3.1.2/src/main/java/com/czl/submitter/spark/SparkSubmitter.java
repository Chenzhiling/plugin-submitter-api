package com.czl.submitter.spark;

import com.czl.submitter.core.entity.*;
import com.czl.submitter.core.enums.ExecutionMode;
import com.czl.submitter.core.exception.SubmitterException;
import com.czl.submitter.core.submitter.AbstractSubmitter;
import com.czl.submitter.spark.entity.SparkQueryRequest;
import com.czl.submitter.spark.entity.SparkStopRequest;
import com.czl.submitter.spark.entity.SparkSubmitRequest;
import com.czl.submitter.spark.service.Impl.StandaloneSubmit;
import com.czl.submitter.spark.service.Impl.YarnSubmit;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/17
 * Description:
 */
public class SparkSubmitter extends AbstractSubmitter {


    @Override
    public SubmitResponse submitTask(SubmitRequest submitRequest) {
        ExecutionMode executionMode = submitRequest.getExecutionMode();
        SparkSubmitRequest request = (SparkSubmitRequest) submitRequest;
        switch (executionMode) {
            case STANDALONE:
                return StandaloneSubmit.submit(request);
            case SPARK_YARN:
                return YarnSubmit.submit(request);
            default:
                throw new SubmitterException("Unsupported mode to submit spark task");
        }
    }


    @Override
    public QueryResponse queryTask(QueryRequest queryRequest) {
        ExecutionMode executionMode = queryRequest.getExecutionMode();
        SparkQueryRequest request = (SparkQueryRequest) queryRequest;
        switch (executionMode) {
            case STANDALONE:
                return StandaloneSubmit.query(request);
            case SPARK_YARN:
                return YarnSubmit.query(request);
            default:
                throw new SubmitterException("Unsupported mode to submit spark task");
        }
    }


    @Override
    public StopResponse stopTask(StopRequest stopRequest) {
        ExecutionMode executionMode = stopRequest.getExecutionMode();
        SparkStopRequest request = (SparkStopRequest) stopRequest;
        switch (executionMode) {
            case STANDALONE:
                return StandaloneSubmit.stop(request);
            case SPARK_YARN:
                return YarnSubmit.stop(request);
            default:
                throw new SubmitterException("Unsupported mode to submit spark task");
        }
    }
}
