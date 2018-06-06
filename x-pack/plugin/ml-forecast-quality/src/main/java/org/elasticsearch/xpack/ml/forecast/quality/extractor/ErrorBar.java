/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.extractor;

import java.util.Date;

public class ErrorBar {

    private final Date timestamp;
    private final double value;
    private final double lower;
    private final double upper;

    ErrorBar(Date timestamp, double value, double lower, double upper) {
        this.timestamp = timestamp;
        this.value = value;
        this.lower = lower;
        this.upper = upper;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public double getLower() {
        return lower;
    }

    public double getUpper() {
        return upper;
    }

}
