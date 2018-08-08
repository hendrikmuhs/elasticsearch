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
import java.util.List;

public class ModelPlotErrorBarExtractor extends AbstractIndexBasedErrorBarExtractor {
    private static final String EPOCH_SECONDS = "epoch_second";

    private final BoolQueryBuilder modelPlotQueryTemplate;

    public ModelPlotErrorBarExtractor(Client client, String indexName, String jobName, Instant startTime, Instant endTime,
            TimeValue bucketSpan) {
        super(client, indexName, startTime, endTime, bucketSpan);
        QueryBuilder termQuery = new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), ModelPlot.RESULT_TYPE_VALUE);
        QueryBuilder jobQuery = new TermsQueryBuilder(Job.ID.getPreferredName(), jobName);
        modelPlotQueryTemplate = new BoolQueryBuilder().filter(termQuery).filter(jobQuery);
    }

    @Override
    public List<ErrorBar> doNext(Instant startBatch, Instant endBatch, int batchSize) {
        QueryBuilder timeQuery = new RangeQueryBuilder(Result.TIMESTAMP.getPreferredName()).gte(startBatch.getEpochSecond())
                .lt(endBatch.getEpochSecond()).format(EPOCH_SECONDS);

        QueryBuilder modelPlotQuery = modelPlotQueryTemplate.filter(timeQuery);

        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS)).setQuery(modelPlotQuery)
                .setSize(batchSize).addSort(Result.TIMESTAMP.getPreferredName(), SortOrder.ASC).get();

        return exractSearchHits(searchResponse, hit -> parse(hit));
    }

    private ErrorBar parse(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        try (InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE, stream)) {
            ModelPlot modelPlot = ModelPlot.LENIENT_PARSER.apply(parser, null);
            return new ErrorBar(modelPlot.getTimestamp(), modelPlot.getActual(), modelPlot.getModelLower(), modelPlot.getModelUpper());
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse modelPlot", e);
        }
    }
}
