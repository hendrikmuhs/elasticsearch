/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.ml.forecast.quality.action.ForecastEvaluateAction.Response;
import org.elasticsearch.xpack.ml.forecast.quality.calculator.AccuracyMeasure;

import java.time.Duration;

public class ForecastEvaluateActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

    @Override
    protected Response createTestInstance() {
        AccuracyMeasure accuracyMeasure = new AccuracyMeasure(randomInt(), Duration.ofMillis(randomLong()), randomDouble(), randomDouble(),
                randomDouble(), randomLong(), randomDouble());
        return new Response(accuracyMeasure, Duration.ofMillis(randomLong()));
    }

}
