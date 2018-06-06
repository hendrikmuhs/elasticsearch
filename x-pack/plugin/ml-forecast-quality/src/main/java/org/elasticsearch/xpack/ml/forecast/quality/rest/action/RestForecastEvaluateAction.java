/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.forecast.quality.action.ForecastEvaluateAction;

import java.io.IOException;

/**
 * Example of adding a cat action with a plugin.
 */
public class RestForecastEvaluateAction extends BaseRestHandler {

    public RestForecastEvaluateAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST,
                "/_xpack/ml/anomaly_detectors/{" + Job.ID.getPreferredName() + "}/_forecast_evaluate", this);
    }

    @Override
    public String getName() {
        return "ml_forecast_evaluate_job_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        String forecastId = restRequest.param(ForecastEvaluateAction.Request.FORECAST_ID.getPreferredName());

        final ForecastEvaluateAction.Request request;
        if (restRequest.hasContentOrSourceParam()) {
            XContentParser parser = restRequest.contentOrSourceParamParser();
            request = ForecastEvaluateAction.Request.parseRequest(jobId, forecastId, parser);
        } else {
            request = new ForecastEvaluateAction.Request(restRequest.param(Job.ID.getPreferredName()),
                    restRequest.param(ForecastEvaluateAction.Request.FORECAST_ID.getPreferredName()));
        }
        return channel -> client.execute(ForecastEvaluateAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
