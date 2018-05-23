/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml_forecastquality.extractor;

import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public interface ErrorBarExtractor {

    List<ErrorBar> next(int batchSize);

    TimeValue getBucketSpan();
}