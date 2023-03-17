package com.czl.submitter.core.submitter;

import com.czl.submitter.core.exception.SubmitterException;

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/16
 * Description:
 */
public class SubmitterFactory {

    public static ISubmitter getSubmitter(String type) {
        try {
            Class<?> aClass = Class.forName(type);
            return (ISubmitter) aClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new SubmitterException(e);
        }
    }
}
