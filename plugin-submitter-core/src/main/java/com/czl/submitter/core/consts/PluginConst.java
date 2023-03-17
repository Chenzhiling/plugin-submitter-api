package com.czl.submitter.core.consts;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/16
 * Description:
 */
public class PluginConst {

    public static final String KEY_YARN_ID = "yarn.application.id";

    public static final String SPARK_MASTER = "spark.master";

    public static final String SPARK_LOCAL_CORE = "spark.local.core";

    public static final String SPARK_DEPLOY_MODE_CLIENT = "client";

    public static final String SPARK_DEPLOY_MODE_CLUSTER = "cluster";

    public static final String SPARK_LAUNCHER_MASTER = "yarn";

    public static final String SPARK_JARS = "spark.jars";

    public static final String SPARK_APP_NAME = "spark.app.name";

    public static final String SPARK_REST_CREATE = "/v1/submissions/create";

    public static final String SPARK_REST_QUERY = "/v1/submissions/status/";

    public static final String SPARK_REST_KILL = "/v1/submissions/kill/";

    public static final String SPARK_PROTOCOL = "spark://";

    public static final String HTTP_PROTOCOL = "http://";
}
