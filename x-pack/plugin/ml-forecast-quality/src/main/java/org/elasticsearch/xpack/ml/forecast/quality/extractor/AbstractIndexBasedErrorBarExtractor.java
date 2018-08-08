/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.forecast.quality.extractor;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class AbstractIndexBasedErrorBarExtractor extends BatchErrorBarExtractor {

    protected static final String EPOCH_SECONDS = "epoch_second";

    protected final Client client;
    protected final String indexName;

    public AbstractIndexBasedErrorBarExtractor(Client client, String indexName, Instant startTime, Instant endTime, TimeValue bucketSpan) {
        super(startTime, endTime, bucketSpan);
        this.client = client;
        this.indexName = indexName;
    }

    protected List<ErrorBar> exractSearchHits(SearchResponse searchResponse, Function<SearchHit, ErrorBar> transform) {

        List<ErrorBar> results = new ArrayList<>();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            results.add(transform.apply(hit));
        }

        return results;
    }
}
