package com.czl.submitter.spark.utils;

import com.czl.submitter.spark.entity.RestSparkResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicResponseHandler;

import java.io.IOException;

/**
 * Author: CHEN ZHI LING
 * Date: 2022/7/25
 * Description:
 */
public class RestSparkResponseUtils {
    public static <T extends RestSparkResponse>  T executeHttpMethodAndGetResponse(HttpClient client,
                                                                                   HttpRequestBase httpRequest,
                                                                                   Class<T> responseClass)
            throws RestSparkRequestException {
        T response;
        try {
            final String stringResponse = client.execute(httpRequest, new BasicResponseHandler());
            if (stringResponse != null) {
                ObjectMapper mapper = new ObjectMapper();
                response = mapper.readValue(stringResponse, responseClass);
            } else {
                throw new RestSparkRequestException("Received empty string response");
            }
        } catch (IOException e) {
            throw new RestSparkRequestException(e);
        } finally {
            httpRequest.releaseConnection();
        }

        if (response == null) {
            throw new RestSparkRequestException("An issue occurred with the cluster's response.");
        }
        return response;
    }
}
