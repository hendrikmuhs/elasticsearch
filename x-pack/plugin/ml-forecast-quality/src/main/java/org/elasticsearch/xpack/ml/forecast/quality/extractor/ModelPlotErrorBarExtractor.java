/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.extractor;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.MlIndicesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ModelPlotErrorBarExtractor extends BatchErrorBarExtractor {
    private static final String EPOCH_SECONDS = "epoch_second";

    private final Client client;
    private final String indexName;
    private final String jobName;

    public ModelPlotErrorBarExtractor(Client client, String indexName, String jobName, Instant startTime, Instant endTime,
            TimeValue bucketSpan) {
        super(startTime, endTime, bucketSpan);
        this.client = client;
        this.indexName = indexName;
        this.jobName = jobName;
    }

    @Override
    public List<ErrorBar> doNext(Instant startBatch, Instant endBatch, int batchSize) {

        SearchResponse searchResponse;
        QueryBuilder timeQuery = new RangeQueryBuilder(Result.TIMESTAMP.getPreferredName()).gte(startBatch.getEpochSecond())
                .lt(endBatch.getEpochSecond()).format(EPOCH_SECONDS);
        QueryBuilder termQuery = new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), ModelPlot.RESULT_TYPE_VALUE);
        QueryBuilder jobQuery = new TermsQueryBuilder(Job.ID.getPreferredName(), jobName);

        QueryBuilder modelPlotQuery = new BoolQueryBuilder().filter(termQuery).filter(jobQuery).filter(timeQuery);

        searchResponse = client.prepareSearch(indexName)
                .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS)).setQuery(modelPlotQuery)
                .setSize(batchSize).addSort(Result.TIMESTAMP.getPreferredName(), SortOrder.ASC).get();

        List<ErrorBar> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            BytesReference source = hit.getSourceRef();
            try (InputStream stream = source.streamInput();
                    XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                            LoggingDeprecationHandler.INSTANCE, stream)) {
                ModelPlot modelPlot = ModelPlot.LENIENT_PARSER.apply(parser, null);
                results.add(new ErrorBar(modelPlot.getTimestamp(), modelPlot.getActual(), modelPlot.getModelLower(),
                        modelPlot.getModelUpper()));
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse modelPlot", e);
            }
        }

        return results;
    }
}
