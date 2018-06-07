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
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.MlIndicesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ForecastErrorBarExtractor extends AbstractIndexBasedErrorBarExtractor {

    private final BoolQueryBuilder forecastResultsQueryTemplate;

    public ForecastErrorBarExtractor(Client client, String indexName, Instant startTime, Instant endTime, TimeValue bucketSpan) {
        super(client, indexName, startTime, endTime, bucketSpan);
        QueryBuilder termQuery = new TermsQueryBuilder(Result.RESULT_TYPE.getPreferredName(), Forecast.RESULT_TYPE_VALUE);
        forecastResultsQueryTemplate = new BoolQueryBuilder().filter(termQuery);
    }

    @Override
    public List<ErrorBar> doNext(Instant startBatch, Instant endBatch, int batchSize) {
        QueryBuilder timeQuery = new RangeQueryBuilder(Result.TIMESTAMP.getPreferredName()).gte(startBatch.getEpochSecond())
                .lt(endBatch.getEpochSecond()).format(EPOCH_SECONDS);
        QueryBuilder forecastQuery = forecastResultsQueryTemplate.filter(timeQuery);

        SearchResponse searchResponse = client.prepareSearch(indexName)
                .setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(SearchRequest.DEFAULT_INDICES_OPTIONS)).setQuery(forecastQuery)
                .setSize(batchSize).get();

        return exractSearchHits(searchResponse, hit -> parse(hit));
    }

    private ErrorBar parse(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        try (InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE, stream)) {
            Forecast forecast = Forecast.STRICT_PARSER.apply(parser, null);
            return new ErrorBar(forecast.getTimestamp(), forecast.getForecastPrediction(), forecast.getForecastLower(),
                    forecast.getForecastUpper());
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse forecast", e);
        }
    }
}
