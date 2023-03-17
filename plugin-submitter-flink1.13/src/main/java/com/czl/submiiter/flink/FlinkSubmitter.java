package com.czl.submiiter.flink;

import com.czl.submiiter.flink.entity.FlinkStopRequest;
import com.czl.submiiter.flink.entity.FlinkSubmitRequest;
import com.czl.submitter.core.entity.*;
import com.czl.submitter.core.enums.ExecutionMode;
import com.czl.submitter.core.exception.SubmitterException;
import com.czl.submitter.core.submitter.AbstractSubmitter;
import com.czl.submitter.flink.service.Impl.RemoteSubmit;
import com.czl.submitter.flink.service.Impl.YarnPerJobSubmit;
import com.czl.submitter.flink.service.Impl.YarnSessionSubmit;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/16
 * Description:
 */
public class FlinkSubmitter extends AbstractSubmitter {

    @Override
    public SubmitResponse submitTask(SubmitRequest submitRequest) {
        ExecutionMode executionMode = submitRequest.getExecutionMode();
        FlinkSubmitRequest request = (FlinkSubmitRequest) submitRequest;
        switch (executionMode) {
            case STANDALONE:
                return RemoteSubmit.submit(request);
            case FLINK_YARN_SESSION:
                return YarnSessionSubmit.submit(request);
            case FLINK_YARN_PER_JOB:
                return YarnPerJobSubmit.submit(request);
            default:
                throw new SubmitterException("Unsupported mode to submit flink task");
        }
    }

    @Override
    public QueryResponse queryTask(QueryRequest queryRequest) {
        ExecutionMode executionMode = queryRequest.getExecutionMode();
        switch (executionMode) {
            case STANDALONE:
                return RemoteSubmit.query(queryRequest);
            case FLINK_YARN_SESSION:
                return YarnSessionSubmit.query(queryRequest);
            case FLINK_YARN_PER_JOB:
                return YarnPerJobSubmit.query(queryRequest);
            default:
                throw new SubmitterException("Unsupported mode to submit flink task");
        }
    }

    @Override
    public StopResponse stopTask(StopRequest stopRequest) {
        ExecutionMode executionMode = stopRequest.getExecutionMode();
        FlinkStopRequest flinkStopRequest = (FlinkStopRequest) stopRequest;
        switch (executionMode) {
            case STANDALONE:
                return RemoteSubmit.stop(flinkStopRequest);
            case FLINK_YARN_SESSION:
                return YarnSessionSubmit.stop(flinkStopRequest);
            case FLINK_YARN_PER_JOB:
                return YarnPerJobSubmit.stop(flinkStopRequest);
            default:
                throw new SubmitterException("Unsupported mode to submit flink task");
        }
    }
}
