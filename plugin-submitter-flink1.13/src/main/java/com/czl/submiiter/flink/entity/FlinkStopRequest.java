package com.czl.submiiter.flink.entity;

import com.czl.submitter.core.entity.StopRequest;
import com.czl.submitter.core.enums.ExecutionMode;
import com.czl.submitter.flink.entity.FlinkInfo;

import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/16
 * Description:
 */
public class FlinkStopRequest extends StopRequest {

    private FlinkInfo flinkInfo;

    private Boolean withSavePoint;

    private String customSavePointPath;

    private Boolean withDrain;

    private Map<String, Object> extraParameter;

    public FlinkInfo getFlinkInfo() {
        return flinkInfo;
    }

    public Boolean getWithSavePoint() {
        return withSavePoint;
    }

    public String getCustomSavePointPath() {
        return customSavePointPath;
    }

    public Boolean getWithDrain() {
        return withDrain;
    }

    public Map<String, Object> getExtraParameter() {
        return extraParameter;
    }

    public FlinkStopRequest(FlinkInfo flinkInfo,
                            ExecutionMode executionMode,
                            String master,
                            String taskId,
                            Boolean withSavePoint,
                            String customSavePointPath,
                            Boolean withDrain,
                            Map<String, Object> extraParameter) {
        super(executionMode, master, taskId);
        this.flinkInfo = flinkInfo;
        this.withSavePoint = withSavePoint;
        this.customSavePointPath = customSavePointPath;
        this.withDrain = withDrain;
        this.extraParameter = extraParameter;
    }
}
