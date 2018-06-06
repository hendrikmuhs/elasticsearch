/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.forecast.quality.action.ForecastEvaluateAction.Request;
import org.elasticsearch.xpack.ml.forecast.quality.action.ForecastEvaluateAction.Response;
import org.elasticsearch.xpack.ml.forecast.quality.calculator.AccuracyCalculator;
import org.elasticsearch.xpack.ml.forecast.quality.calculator.AccuracyMeasure;
import org.elasticsearch.xpack.ml.forecast.quality.extractor.ForecastErrorBarExtractor;
import org.elasticsearch.xpack.ml.forecast.quality.extractor.ModelPlotErrorBarExtractor;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportForecastEvaluateAction
        extends HandledTransportAction<ForecastEvaluateAction.Request, ForecastEvaluateAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportForecastEvaluateAction(Settings settings, String actionName, ThreadPool threadPool, TransportService transportService,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService,
            Client client) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                ForecastEvaluateAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        threadPool.executor(Names.GENERIC).execute(() -> {
            new AsyncForecastEvaluateAction(request, listener).start();
        });
    }

    class AsyncForecastEvaluateAction {

        private final Request request;
        private final ActionListener<Response> listener;

        private final long startTime;

        AsyncForecastEvaluateAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
            this.startTime = threadPool.relativeTimeInMillis();
        }

        public void start() {
            ClusterState clusterState = clusterService.state();
            MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
            
            // INTEGRATION: use JobManager.getJobOrThrowIfUnknown
            Job job = mlMetadata.getJobs().get(request.getJobId());

            if (job == null) {
                listener.onFailure(new ResourceNotFoundException("Job with id [{}] not found", request.getJobId()));
                return;
            }

            if (request.getForecastId() == null) {
                listener.onFailure(new ElasticsearchException("Required parameter forecast_id missing"));
                return;
            }

            ModelPlotConfig modelPlotConfig = job.getModelPlotConfig();
            if (modelPlotConfig == null || modelPlotConfig.isEnabled() == false) {
                listener.onFailure(new ElasticsearchException("Job [{}] has no model plot.", request.getJobId()));
                return;
            }

            getForecastRequestStats(request.getJobId(), request.getForecastId(), forecastRequestStats -> {
                if (forecastRequestStats == null) {
                    listener.onFailure(new ResourceNotFoundException("Forecast with id [{}] not found", request.getForecastId()));
                } else if (forecastRequestStats.getStatus() == ForecastRequestStats.ForecastRequestStatus.FAILED) {
                    listener.onFailure(new ResourceNotFoundException("Forecast with id [{}] failed", request.getForecastId()));
                } else {
                    job.getResultsIndexName();
                    Instant startTime = forecastRequestStats.getStartTime();
                    Instant endTime = forecastRequestStats.getEndTime();
                    ModelPlotErrorBarExtractor modelPlotExtractor = new ModelPlotErrorBarExtractor(client, job.getResultsIndexName(),
                            job.getId(), startTime, endTime, job.getAnalysisConfig().getBucketSpan());
                    ForecastErrorBarExtractor forecastExtractor = new ForecastErrorBarExtractor(client, job.getResultsIndexName(),
                            startTime, endTime, job.getAnalysisConfig().getBucketSpan());

                    AccuracyMeasure scores = AccuracyCalculator.compare(modelPlotExtractor, forecastExtractor);
                    long took = threadPool.relativeTimeInMillis() - this.startTime;
                    listener.onResponse(new ForecastEvaluateAction.Response(scores, took));
                }
            }, errorResponse -> {
                listener.onFailure(errorResponse);
            });
        }
        
        // INTEGRATION: all 3 methods are duplicated from JobProvider, to be re-factored
        private void getForecastRequestStats(String jobId, String forecastId, Consumer<ForecastRequestStats> handler,
                Consumer<Exception> errorHandler) {
            String indexName = AnomalyDetectorsIndex.jobResultsAliasedName(jobId);
            GetRequest getRequest = new GetRequest(indexName, ElasticsearchMappings.DOC_TYPE,
                    ForecastRequestStats.documentId(jobId, forecastId));

            getResult(jobId, ForecastRequestStats.RESULTS_FIELD.getPreferredName(), getRequest, ForecastRequestStats.LENIENT_PARSER,
                    result -> handler.accept(result.result), errorHandler, () -> null);
        }

        private <U, T> void getResult(String jobId, String resultDescription, GetRequest get, BiFunction<XContentParser, U, T> objectParser,
                Consumer<Result<T>> handler, Consumer<Exception> errorHandler, Supplier<T> notFoundSupplier) {

            executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, get,
                    ActionListener.<GetResponse>wrap(getDocResponse -> {
                        if (getDocResponse.isExists()) {
                            handler.accept(
                                    new Result<>(getDocResponse.getIndex(), parseGetHit(getDocResponse, objectParser, errorHandler)));
                        } else {
                            handler.accept(new Result<>(null, notFoundSupplier.get()));
                        }
                    }, errorHandler), client::get);
        }

        private <T, U> T parseGetHit(GetResponse getResponse, BiFunction<XContentParser, U, T> objectParser,
                Consumer<Exception> errorHandler) {
            BytesReference source = getResponse.getSourceAsBytesRef();

            try (InputStream stream = source.streamInput();
                    XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                            LoggingDeprecationHandler.INSTANCE, stream)) {
                return objectParser.apply(parser, null);
            } catch (IOException e) {
                errorHandler.accept(new ElasticsearchParseException("failed to parse " + getResponse.getType(), e));
                return null;
            }
        }
    }

}
