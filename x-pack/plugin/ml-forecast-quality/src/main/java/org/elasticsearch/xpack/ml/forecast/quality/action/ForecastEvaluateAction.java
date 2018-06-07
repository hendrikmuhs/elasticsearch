/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.forecast.quality.calculator.AccuracyMeasure;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

public class ForecastEvaluateAction extends Action<ForecastEvaluateAction.Request, ForecastEvaluateAction.Response> {

    public static final ForecastEvaluateAction INSTANCE = new ForecastEvaluateAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/forecast_evaluate";

    private ForecastEvaluateAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField FORECAST_ID = new ParseField("forecast_id");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString((request, forecastId) -> request.forecastId = forecastId, FORECAST_ID);
        }

        public static Request parseRequest(String jobId, String forecastId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            if (forecastId != null) {
                request.forecastId = forecastId;
            }
            return request;
        }

        private String jobId;
        private String forecastId;

        public Request() {
        }

        public Request(String jobId, String forecastId) {
            super();
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
            this.forecastId = ExceptionsHelper.requireNonNull(forecastId, FORECAST_ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public String getForecastId() {
            return forecastId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.jobId = in.readString();
            this.forecastId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeString(forecastId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, forecastId);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(jobId, other.jobId) && Objects.equals(forecastId, other.forecastId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(FORECAST_ID.getPreferredName(), forecastId);
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            return validationException;
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        RequestBuilder(ElasticsearchClient client, ForecastEvaluateAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private int count;
        private Duration timeSpan;
        private Duration took;
        private double meanAbsoluteError;
        private double meanAbsolutePercentageError;
        private double medianAbsolutePercentageError;

        public Response() {
            super();
        }

        public Response(AccuracyMeasure accuracyMeasure, Duration took) {
            super();
            this.count = accuracyMeasure.getCount();
            this.timeSpan = accuracyMeasure.getTimeSpan();
            this.meanAbsoluteError = accuracyMeasure.getMeanAbsoluteError();
            this.meanAbsolutePercentageError = accuracyMeasure.getMeanAbsolutePercentageError();
            this.medianAbsolutePercentageError = accuracyMeasure.getMedianAbsolutePercentageError();
            this.took = took;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.count = in.readInt();
            this.timeSpan = Duration.ofMillis(in.readLong());
            this.meanAbsoluteError = in.readDouble();
            this.meanAbsolutePercentageError = in.readDouble();
            this.medianAbsolutePercentageError = in.readDouble();
            this.took = Duration.ofMillis(in.readLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(count);
            out.writeLong(timeSpan.toMillis());
            out.writeDouble(meanAbsoluteError);
            out.writeDouble(meanAbsolutePercentageError);
            out.writeDouble(medianAbsolutePercentageError);
            out.writeLong(took.toMillis());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("count", count);
            builder.field("time_span", timeSpan.toMillis());
            builder.field("mean_absolute_error", meanAbsoluteError);
            builder.field("mean_absolute_percentage_error", meanAbsolutePercentageError);
            builder.field("median_absolute_percentage_error", medianAbsolutePercentageError);
            builder.field("took", took.toMillis());
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return this.count == other.count && this.timeSpan == other.timeSpan && this.took == other.took
                    && this.meanAbsoluteError == other.meanAbsoluteError
                    && this.meanAbsolutePercentageError == other.meanAbsolutePercentageError
                    && this.medianAbsolutePercentageError == other.medianAbsolutePercentageError;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, timeSpan, meanAbsoluteError, meanAbsolutePercentageError, medianAbsolutePercentageError, took);
        }
    }
}
