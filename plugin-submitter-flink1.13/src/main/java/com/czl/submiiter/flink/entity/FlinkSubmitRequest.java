package com.czl.submiiter.flink.entity;

import com.czl.submiiter.flink.enums.ResolveOrder;
import com.czl.submitter.core.entity.SubmitRequest;
import com.czl.submitter.core.enums.ExecutionMode;
import com.czl.submitter.flink.entity.FlinkInfo;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/15
 * Description:
 */
public class FlinkSubmitRequest extends SubmitRequest {

    private FlinkInfo flinkInfo;

    private String savePoint;

    private ResolveOrder resolveOrder;

    private int flinkParallelism;

    public FlinkSubmitRequest(FlinkInfo flinkInfo,
                              ExecutionMode executionMode,
                              ResolveOrder resolveOrder,
                              String appName,
                              String mainClass,
                              String jarPath,
                              String savePoint,
                              int flinkParallelism,
                              List<String> args,
                              Map<String, Object> extraParameter) {
        super(executionMode, appName, mainClass, jarPath, args, extraParameter);
        this.flinkInfo = flinkInfo;
        this.savePoint = savePoint;
        this.resolveOrder = resolveOrder;
        this.flinkParallelism = flinkParallelism;
    }

    public FlinkInfo getFlinkInfo() {
        return flinkInfo;
    }

    public String getSavePoint() {
        return savePoint;
    }

    public ResolveOrder getResolveOrder() {
        return resolveOrder;
    }

    public int getFlinkParallelism() {
        return flinkParallelism;
    }

    public String effectiveName() {
        return null == this.getAppName()? "TrapaLake" : this.getAppName();
    }

    public File supportTaskJarFile() {
        return new File(this.getJarPath());
    }


    public SavepointRestoreSettings getSavepointRestoreSettings() {
        boolean allowNonRestoredState = Boolean.parseBoolean(
                this.getExtraParameter().getOrDefault(
                        SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE.key(), false).toString());
        return null == this.savePoint ?
                SavepointRestoreSettings.none() :
                SavepointRestoreSettings.forPath(this.savePoint, allowNonRestoredState);
    }
}
