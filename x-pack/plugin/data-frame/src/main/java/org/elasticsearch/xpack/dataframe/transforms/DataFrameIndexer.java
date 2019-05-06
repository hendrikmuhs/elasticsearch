/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class DataFrameIndexer extends AsyncTwoPhaseIndexer<Map<String, Object>, DataFrameIndexerTransformStats> {

    public static final int MINIMUM_PAGE_SIZE = 10;
    public static final String COMPOSITE_AGGREGATION_NAME = "_data_frame";
    private static final Logger logger = LogManager.getLogger(DataFrameIndexer.class);

    protected final DataFrameAuditor auditor;

    protected final DataFrameTransformConfig transformConfig;
    protected volatile DataFrameTransformProgress progress;
    private final Map<String, String> fieldMappings;

    private Pivot pivot;
    private int pageSize = 0;
    protected volatile DataFrameTransformCheckpoint inProgressCheckpoint;
    private volatile Map<String, List<String>> changedBuckets;

    public DataFrameIndexer(Executor executor,
                            DataFrameAuditor auditor,
                            DataFrameTransformConfig transformConfig,
                            Map<String, String> fieldMappings,
                            AtomicReference<IndexerState> initialState,
                            Map<String, Object> initialPosition,
                            DataFrameIndexerTransformStats jobStats,
                            DataFrameTransformProgress transformProgress,
                            DataFrameTransformCheckpoint inProgressCheckpoint) {
        super(executor, initialState, initialPosition, jobStats);
        this.auditor = Objects.requireNonNull(auditor);
        this.transformConfig = ExceptionsHelper.requireNonNull(transformConfig, "transformConfig");
        this.fieldMappings = ExceptionsHelper.requireNonNull(fieldMappings, "fieldMappings");
        this.progress = transformProgress;
        this.inProgressCheckpoint = inProgressCheckpoint;
    }

    protected abstract void failIndexer(String message);

    public int getPageSize() {
        return pageSize;
    }

    public DataFrameTransformConfig getConfig() {
        return transformConfig;
    }

    public boolean isContinuous() {
        return getConfig().getSyncConfig() != null;
    }

    public Map<String, String> getFieldMappings() {
        return fieldMappings;
    }

    public DataFrameTransformProgress getProgress() {
        return progress;
    }

    /**
     * Request a checkpoint
     */
    protected abstract void createCheckpoint(ActionListener<DataFrameTransformCheckpoint> listener);

    @Override
    protected void onStart(long now, ActionListener<Void> listener) {
        try {
            pivot = new Pivot(getConfig().getPivotConfig());

            // if we haven't set the page size yet, if it is set we might have reduced it after running into an out of memory
            if (pageSize == 0) {
                pageSize = pivot.getInitialPageSize();
            }

            // if run for the 1st time, create checkpoint
            if (initialRun()) {
                createCheckpoint(ActionListener.wrap(cp -> {
                    if (inProgressCheckpoint.isEmpty() == false) {
                        getChangedBuckets(inProgressCheckpoint, cp, ActionListener.wrap(r -> {

                            inProgressCheckpoint = cp;

                            logger.info("created checkpoint");
                            listener.onResponse(null);
                        }, listener::onFailure));
                    } else {
                        inProgressCheckpoint = cp;

                        logger.info("created checkpoint");
                        listener.onResponse(null);
                    }
                }, listener::onFailure));
            } else {
                listener.onResponse(null);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected boolean initialRun() {
        return getPosition() == null;
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        // reset the page size, so we do not memorize a low page size forever, the pagesize will be re-calculated on start
        pageSize = 0;
        changedBuckets = null;
    }

    @Override
    protected IterationResult<Map<String, Object>> doProcess(SearchResponse searchResponse) {
        final CompositeAggregation agg = searchResponse.getAggregations().get(COMPOSITE_AGGREGATION_NAME);

        // we reached the end
        if (agg.getBuckets().isEmpty()) {
            return new IterationResult<>(Collections.emptyList(), null, true);
        }

        long docsBeforeProcess = getStats().getNumDocuments();
        IterationResult<Map<String, Object>> result = new IterationResult<>(processBucketsToIndexRequests(agg).collect(Collectors.toList()),
            agg.afterKey(),
            agg.getBuckets().isEmpty());
        if (progress != null) {
            progress.docsProcessed(getStats().getNumDocuments() - docsBeforeProcess);
        }
        return result;
    }

    /*
     * Parses the result and creates a stream of indexable documents
     *
     * Implementation decisions:
     *
     * Extraction uses generic maps as intermediate exchange format in order to hook in ingest pipelines/processors
     * in later versions, see {@link IngestDocument).
     */
    private Stream<IndexRequest> processBucketsToIndexRequests(CompositeAggregation agg) {
        final DataFrameTransformConfig transformConfig = getConfig();
        String indexName = transformConfig.getDestination().getIndex();

        return pivot.extractResults(agg, getFieldMappings(), getStats()).map(document -> {
            String id = (String) document.get(DataFrameField.DOCUMENT_ID_FIELD);

            if (id == null) {
                throw new RuntimeException("Expected a document id but got null.");
            }

            XContentBuilder builder;
            try {
                builder = jsonBuilder();
                builder.startObject();
                for (Map.Entry<String, ?> value : document.entrySet()) {
                    // skip all internal fields
                    if (value.getKey().startsWith("_") == false) {
                        builder.field(value.getKey(), value.getValue());
                    }
                }
                builder.endObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            IndexRequest request = new IndexRequest(indexName).source(builder).id(id);
            return request;
        });
    }

    @Override
    protected SearchRequest buildSearchRequest() {
        SearchRequest searchRequest = new SearchRequest(getConfig().getSource().getIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(pivot.buildAggregation(getPosition(), pageSize));
        sourceBuilder.size(0);

        QueryBuilder pivotQueryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        DataFrameTransformConfig config = getConfig();
        if (config.getSyncConfig() != null) {
            if (inProgressCheckpoint == null) {
                throw new RuntimeException("in progress checkpoint not found");
            }

            BoolQueryBuilder filteredQuery = new BoolQueryBuilder().
                    filter(pivotQueryBuilder).
                    filter(config.getSyncConfig().getBoundaryQuery(inProgressCheckpoint));

            if (changedBuckets != null && changedBuckets.isEmpty() == false) {
                QueryBuilder pivotFilter = pivot.filterBuckets(changedBuckets);
                if (pivotFilter != null) {
                    filteredQuery.filter(pivotFilter);
                }
            }

            logger.info("running filtered query: " + filteredQuery);
            sourceBuilder.query(filteredQuery);
        } else {
            sourceBuilder.query(pivotQueryBuilder);
        }

        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    /**
     * Handle the circuit breaking case: A search consumed to much memory and got aborted.
     *
     * Going out of memory we smoothly reduce the page size which reduces memory consumption.
     *
     * Implementation details: We take the values from the circuit breaker as a hint, but
     * note that it breaks early, that's why we also reduce using
     *
     * @param e Exception thrown, only {@link CircuitBreakingException} are handled
     * @return true if exception was handled, false if not
     */
    protected boolean handleCircuitBreakingException(Exception e) {
        CircuitBreakingException circuitBreakingException = getCircuitBreakingException(e);

        if (circuitBreakingException == null) {
            return false;
        }

        double reducingFactor = Math.min((double) circuitBreakingException.getByteLimit() / circuitBreakingException.getBytesWanted(),
                1 - (Math.log10(pageSize) * 0.1));

        int newPageSize = (int) Math.round(reducingFactor * pageSize);

        if (newPageSize < MINIMUM_PAGE_SIZE) {
            String message = DataFrameMessages.getMessage(DataFrameMessages.LOG_DATA_FRAME_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE, pageSize);
            failIndexer(message);
            return true;
        }

        String message = DataFrameMessages.getMessage(DataFrameMessages.LOG_DATA_FRAME_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE, pageSize,
                newPageSize);
        auditor.info(getJobId(), message);
        logger.info("Data frame transform [" + getJobId() + "]:" + message);

        pageSize = newPageSize;
        return true;
    }

    private void getChangedBuckets(DataFrameTransformCheckpoint oldCheckpoint, DataFrameTransformCheckpoint newCheckpoint,
            ActionListener<Void> listener) {
        SearchRequest searchRequest = new SearchRequest(getConfig().getSource().getIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // we do not need the sub-aggs
        CompositeAggregationBuilder changesAgg = pivot.buildChangesAggregation(null, pageSize);
        sourceBuilder.aggregation(changesAgg);
        sourceBuilder.size(0);

        QueryBuilder pivotQueryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        DataFrameTransformConfig config = getConfig();
        if (config.getSyncConfig() != null) {
            if (inProgressCheckpoint == null) {
                throw new RuntimeException("in progress checkpoint not found");
            }

            BoolQueryBuilder filteredQuery = new BoolQueryBuilder().
                    filter(pivotQueryBuilder).
                    filter(config.getSyncConfig().getChangesQuery(oldCheckpoint, newCheckpoint));

            logger.info("changes query: " + filteredQuery);
            sourceBuilder.query(filteredQuery);
        } else {
            logger.info("found no sync configuration");
            listener.onResponse(null);
            return;
        }

        searchRequest.source(sourceBuilder);
        searchRequest.allowPartialSearchResults(false);

        Map<String, List<String>> keys = new HashMap<>();

        collectChangedBuckets(searchRequest, changesAgg, keys, ActionListener.wrap(allKeys -> {
            logger.info("changed keys" + allKeys);
            changedBuckets = allKeys;
            listener.onResponse(null);
        }, listener::onFailure));
    }

    void collectChangedBuckets(SearchRequest searchRequest, CompositeAggregationBuilder changesAgg, Map<String, List<String>> keys,
            ActionListener<Map<String, List<String>>> finalListener) {
        doNextSearch(searchRequest, ActionListener.wrap(searchResponse -> {
            long numberOfHits = searchResponse.getHits().getTotalHits().value;
            final CompositeAggregation agg = searchResponse.getAggregations().get(COMPOSITE_AGGREGATION_NAME);
            agg.getBuckets().stream().forEach(bucket -> {
                bucket.getKey().forEach((k, v) -> {
                    logger.info("key " + k + " value " + v);
                    keys.computeIfAbsent(k, l -> new ArrayList<>()).add(v.toString());
                });
            });

            logger.info("found buckets: " + keys.size());

            if (numberOfHits == keys.size()) {
                // adjust the after key
                changesAgg.aggregateAfter(agg.afterKey());
                collectChangedBuckets(searchRequest, changesAgg, keys, finalListener);
            } else {
                logger.info("changed keys" + keys);
                finalListener.onResponse(keys);
            }
        }, finalListener::onFailure));
    }

    /**
     * Inspect exception for circuit breaking exception and return the first one it can find.
     *
     * @param e Exception
     * @return CircuitBreakingException instance if found, null otherwise
     */
    private static CircuitBreakingException getCircuitBreakingException(Exception e) {
        // circuit breaking exceptions are at the bottom
        Throwable unwrappedThrowable = org.elasticsearch.ExceptionsHelper.unwrapCause(e);

        if (unwrappedThrowable instanceof CircuitBreakingException) {
            return (CircuitBreakingException) unwrappedThrowable;
        } else if (unwrappedThrowable instanceof SearchPhaseExecutionException) {
            SearchPhaseExecutionException searchPhaseException = (SearchPhaseExecutionException) e;
            for (ShardSearchFailure shardFailure : searchPhaseException.shardFailures()) {
                Throwable unwrappedShardFailure = org.elasticsearch.ExceptionsHelper.unwrapCause(shardFailure.getCause());

                if (unwrappedShardFailure instanceof CircuitBreakingException) {
                    return (CircuitBreakingException) unwrappedShardFailure;
                }
            }
        }

        return null;
    }

    protected abstract boolean sourceHasChanged();
}
