package com.czl.submitter.core.submitter;

import com.czl.submitter.core.entity.*;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/15
 * Description:
 */
public interface ISubmitter {

    SubmitResponse submitTask(SubmitRequest submitRequest);


    QueryResponse queryTask(QueryRequest queryRequest);


    StopResponse stopTask(StopRequest stopRequest);
}
