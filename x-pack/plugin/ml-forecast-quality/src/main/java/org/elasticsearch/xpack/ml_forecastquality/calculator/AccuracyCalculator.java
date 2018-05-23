/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml_forecastquality.calculator;

import org.elasticsearch.xpack.ml_forecastquality.extractor.ErrorBar;
import org.elasticsearch.xpack.ml_forecastquality.extractor.ErrorBarExtractor;

import java.util.ArrayList;
import java.util.List;

public class AccuracyCalculator {

    private static final int BATCH_SIZE = 100;

    public static AccuracyMeasure compare(ErrorBarExtractor baseline, ErrorBarExtractor prediction) {

        List<ErrorBar> baselineValues;
        List<ErrorBar> predictionValues;

        List<Double> actuals = new ArrayList<Double>(BATCH_SIZE);
        List<Double> compareValues = new ArrayList<Double>(BATCH_SIZE);
        List<Double> errors = new ArrayList<Double>(BATCH_SIZE);
        List<Double> absoluteErrors = new ArrayList<Double>(BATCH_SIZE);

        List<Double> relativeAbsoluteErrors = new ArrayList<Double>(BATCH_SIZE);
        List<Double> symetricAbsolutePercentageErrors = new ArrayList<Double>(BATCH_SIZE);
        List<Double> maseDenominator = new ArrayList<Double>(BATCH_SIZE);
        List<Double> maseSmoothedErrors = new ArrayList<Double>(BATCH_SIZE);

        int totalCount = 0;
        do {
            baselineValues = baseline.next(BATCH_SIZE);
            predictionValues = prediction.next(BATCH_SIZE);

            int numberOfDataPoints = baselineValues.size();

            if (numberOfDataPoints > predictionValues.size()) {
                numberOfDataPoints = predictionValues.size();
            }

            double baselinePredecessor = 0;

            for (int i = 0; i < numberOfDataPoints; ++i) {
                double baselineValue = baselineValues.get(i).getValue();
                double compareValue = predictionValues.get(i).getValue();

                actuals.add(baselineValue);
                compareValues.add(compareValue);
                double error = baselineValue - compareValue;
                double relativeError = (baselineValue - compareValue) / baselineValue;

                errors.add(error);
                absoluteErrors.add(Math.abs(error));

                // for MAPE and MdAPE
                relativeAbsoluteErrors.add(Math.abs(relativeError));

                // for SMAPE
                symetricAbsolutePercentageErrors.add(Math.abs(error) / (Math.abs(baselineValue) + Math.abs(compareValue) / 2));

                if (totalCount > 0) {
                    // for MASE
                    maseDenominator.add(Math.abs(baselineValue - baselinePredecessor));
                    maseSmoothedErrors.add(Math.abs(error) / calculateMean(maseDenominator));
                }
                ++totalCount;
                baselinePredecessor = baselineValue;
            }

        } while (baselineValues.isEmpty() == false);

        double meanAbsoluteError = calculateMean(absoluteErrors);
        double meanAbsolutePercentageError = calculateMean(relativeAbsoluteErrors);

        relativeAbsoluteErrors.sort((a, b) -> Double.compare(a, b));
        double medianAbsolutePercentageError = calculateMedian(relativeAbsoluteErrors);

        double symetricMeanAbsolutePercentageError = calculateMean(symetricAbsolutePercentageErrors);
        double meanAbsoluteScaledError = calculateMean(maseSmoothedErrors);
        long analyzedTimeInSeconds = baseline.getBucketSpan().getSeconds() * totalCount;

        return new AccuracyMeasure(totalCount, analyzedTimeInSeconds, meanAbsoluteError, meanAbsolutePercentageError,
                medianAbsolutePercentageError, symetricMeanAbsolutePercentageError, meanAbsoluteScaledError);
    }

    private static double calculateMedian(List<Double> sortedValues) {
        int middle = sortedValues.size() / 2;

        return (sortedValues.size() % 2 == 1) ? sortedValues.get(middle) : (sortedValues.get(middle - 1) + sortedValues.get(middle)) / 2.0;
    }

    private static double calculateMean(List<Double> values) {
        return values.stream().mapToDouble(val -> val).average().orElse(0.0);
    }

}
