/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml_forecastquality.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml_forecastquality.action.ForecastEvaluateAction.Request;

public class ForecastEvaluateActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, null, parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
