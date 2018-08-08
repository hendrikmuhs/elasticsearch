/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.calculator;

import java.time.Duration;

/**
 * Utility class to hold results from the accuracy calculation
 */
public class AccuracyMeasure {
    private final int count;
    private final Duration timeSpan;
    private final double meanAbsoluteError;
    private final double meanAbsolutePercentageError;
    private final double medianAbsolutePercentageError;
    private final double symetricMeanAbsolutePercentageError;
    private final double meanAbsoluteScaledError;

    public AccuracyMeasure(int count, Duration timeSpan, double meanAbsoluteError, double meanAbsolutePercentageError,
            double medianAbsolutePercentageError, double symetricmeanAbsolutePercentageError, double meanAbsoluteScaledError) {
        this.count = count;
        this.timeSpan = timeSpan;
        this.meanAbsoluteError = meanAbsoluteError;
        this.meanAbsolutePercentageError = meanAbsolutePercentageError;
        this.medianAbsolutePercentageError = medianAbsolutePercentageError;
        this.symetricMeanAbsolutePercentageError = symetricmeanAbsolutePercentageError;
        this.meanAbsoluteScaledError = meanAbsoluteScaledError;
    }

    public int getCount() {
        return count;
    }

    public Duration getTimeSpan() {
        return timeSpan;
    }

    public double getMeanAbsoluteError() {
        return meanAbsoluteError;
    }

    public double getMeanAbsolutePercentageError() {
        return meanAbsolutePercentageError;
    }

    public double getMedianAbsolutePercentageError() {
        return medianAbsolutePercentageError;
    }

    public double getMeanAbsoluteScaledError() {
        return meanAbsoluteScaledError;
    }

    public double getSymetricMeanAbsolutePercentageError() {
        return symetricMeanAbsolutePercentageError;
    }
}
