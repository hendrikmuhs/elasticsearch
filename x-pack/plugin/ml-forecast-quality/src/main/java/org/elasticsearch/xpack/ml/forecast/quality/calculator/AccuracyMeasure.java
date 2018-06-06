/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.calculator;

public class AccuracyMeasure {
    private final int count;
    private final long timeSpanInMillis;
    private final double meanAbsoluteError;
    private final double meanAbsolutePercentageError;
    private final double medianAbsolutePercentageError;
    private final double symetricMeanAbsolutePercentageError;
    private final double meanAbsoluteScaledError;

    public AccuracyMeasure(int count, long timeSpanInSeconds, double meanAbsoluteError, double meanAbsolutePercentageError,
            double medianAbsolutePercentageError, double symetricmeanAbsolutePercentageError, double meanAbsoluteScaledError) {
        this.count = count;
        this.timeSpanInMillis = timeSpanInSeconds;
        this.meanAbsoluteError = meanAbsoluteError;
        this.meanAbsolutePercentageError = meanAbsolutePercentageError;
        this.medianAbsolutePercentageError = medianAbsolutePercentageError;
        this.symetricMeanAbsolutePercentageError = symetricmeanAbsolutePercentageError;
        this.meanAbsoluteScaledError = meanAbsoluteScaledError;
    }

    public int getCount() {
        return count;
    }

    public long getTimeSpanInSeconds() {
        return timeSpanInMillis;
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
