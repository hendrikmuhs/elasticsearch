/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.extractor;

import org.elasticsearch.common.unit.TimeValue;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

public abstract class BatchErrorBarExtractor implements ErrorBarExtractor {

    private final Instant startTime;
    private final Instant endTime;
    private final TimeValue bucketSpan;

    private Instant timeCursor;

    public BatchErrorBarExtractor(Instant startTime, Instant endTime, TimeValue bucketSpan) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.bucketSpan = bucketSpan;
    }

    @Override
    public List<ErrorBar> next(int batchSize) {
        if (timeCursor == null) {
            timeCursor = startTime;
        } else if (timeCursor == endTime) {
            return Collections.emptyList();
        }

        Instant endBatch = timeCursor.plusMillis(batchSize * bucketSpan.millis());
        if (endBatch.isBefore(endTime) == false) {
            endBatch = endTime;
        }

        List<ErrorBar> results = doNext(timeCursor, endBatch, batchSize);

        timeCursor = endBatch;

        return results;
    }

    @Override
    public TimeValue getBucketSpan() {
        return bucketSpan;
    }

    public abstract List<ErrorBar> doNext(Instant startBatch, Instant endBatch, int batchSize);

}
