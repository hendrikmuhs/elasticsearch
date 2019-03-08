/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class DataFrameTransformsCheckpointService {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformsCheckpointService.class);

    private final Client client;

    public DataFrameTransformsCheckpointService(final Client client) {
        this.client = client;
    }

    public void getCheckpoint(DataFrameTransformConfig transformConfig, ActionListener<DataFrameTransformCheckpoint> listener) {
        getCheckpoint(transformConfig, -1L, listener);
    }

    public void getCheckpoint(DataFrameTransformConfig transformConfig, long checkpointId,
            ActionListener<DataFrameTransformCheckpoint> listener) {
        long timestamp = System.currentTimeMillis();

        // placeholder for time based synchronization
        long timeUpperBound = 0;

        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client,
                IndicesStatsAction.INSTANCE, new IndicesStatsRequest().indices(transformConfig.getSource()),
                ActionListener.wrap(response -> {
                    Map<String, long[]> checkpointsByIndex = extractIndexCheckPoints(response.getIndices());
                    DataFrameTransformCheckpoint checkpointDoc = new DataFrameTransformCheckpoint(transformConfig.getId(), timestamp,
                            checkpointId, checkpointsByIndex, timeUpperBound);
                    listener.onResponse(checkpointDoc);

                }, IndicesStatsRequestException -> {
                    throw new RuntimeException("Failed to retrieve indices stats", IndicesStatsRequestException);
                }));
    }

    private static Map<String, long[]> extractIndexCheckPoints(Map<String, IndexStats> indexStatsByIndex) {
        Map<String, long[]> checkpointsByIndex = new TreeMap<>();
        for (Entry<String, IndexStats> stats : indexStatsByIndex.entrySet()) {
            String indexName = stats.getKey();
            List<Long> checkpoints = new ArrayList<>();
            for (IndexShardStats indexShardStats : stats.getValue()) {
                for (ShardStats shardStats : indexShardStats.getShards()) {
                    // we take the global checkpoint, which is consistent across all replicas
                    checkpoints.add(shardStats.getSeqNoStats().getGlobalCheckpoint());
                }
            }
            checkpointsByIndex.put(indexName, checkpoints.stream().mapToLong(l -> l).toArray());
        }
        return checkpointsByIndex;
    }
}
