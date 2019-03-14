/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * DataFrameTransform Checkpoint Service
 *
 * Allows checkpointing a source of a data frame transform which includes all relevant checkpoints of the source.
 *
 * This will be used to checkpoint a transform, detect changes, run the transform in continuous mode.
 *
 */
public class DataFrameTransformsCheckpointService {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformsCheckpointService.class);

    private final Client client;

    public DataFrameTransformsCheckpointService(final Client client) {
        this.client = client;
    }

    /**
     * Get a checkpoint that is not attached to an id. E.g. for change detection.
     *
     * @param transformConfig the @link{DataFrameTransformConfig}
     * @param listener listener to call after inner request returned
     */
    public void getCheckpoint(DataFrameTransformConfig transformConfig, ActionListener<DataFrameTransformCheckpoint> listener) {
        getCheckpoint(transformConfig, -1L, listener);
    }

    /**
     * Get a checkpoint with an id, used to store a checkpoint.
     *
     * @param transformConfig the @link{DataFrameTransformConfig}
     * @param listener listener to call after inner request returned
     */
    public void getCheckpoint(DataFrameTransformConfig transformConfig, long checkpointId,
            ActionListener<DataFrameTransformCheckpoint> listener) {
        long timestamp = System.currentTimeMillis();

        // placeholder for time based synchronization
        long timeUpperBound = 0;

        ClientHelper.executeWithHeadersAsync(transformConfig.getHeaders(), ClientHelper.DATA_FRAME_ORIGIN, client,
                IndicesStatsAction.INSTANCE, new IndicesStatsRequest().indices(transformConfig.getSource()),
                ActionListener.wrap(response -> {
                    if (response.getFailedShards() != 0) {
                        throw new CheckpointException("Source has [" + response.getFailedShards() + "] failed shards");
                    }

                    Map<String, long[]> checkpointsByIndex = extractIndexCheckPoints(response.getShards());
                    DataFrameTransformCheckpoint checkpointDoc = new DataFrameTransformCheckpoint(transformConfig.getId(), timestamp,
                            checkpointId, checkpointsByIndex, timeUpperBound);
                    listener.onResponse(checkpointDoc);

                }, IndicesStatsRequestException -> {
                    throw new CheckpointException("Failed to retrieve indices stats", IndicesStatsRequestException);
                }));
    }

    static Map<String, long[]> extractIndexCheckPoints(ShardStats[] shards) {
        Map<String, long[]> checkpointsByIndex = new TreeMap<>();

        // 1st pass get all indices
        Set<Index> indices = new HashSet<>();
        for (ShardStats shard : shards) {
            indices.add(shard.getShardRouting().index());
        }

        // 2nd pass
        for (Index index : indices) {
            TreeMap<Integer, Long> checkpoints = new TreeMap<>();
            String indexName = index.getName();
            for (ShardStats shard : shards) {
                if (shard.getShardRouting().getIndexName().equals(indexName)) {
                    // note: in case of replicas this gets overridden
                    if (checkpoints.containsKey(shard.getShardRouting().getId())) {
                        if (checkpoints.get(shard.getShardRouting().getId()) != shard.getSeqNoStats().getGlobalCheckpoint()) {
                            throw new CheckpointException("Global checkpoints mismatch for index [" + indexName + "] between shards of id ["
                                    + shard.getShardRouting().getId() + "]");
                        }
                    } else {
                        checkpoints.put(shard.getShardRouting().getId(), shard.getSeqNoStats().getGlobalCheckpoint());
                    }
                }
            }
            checkpointsByIndex.put(indexName, checkpoints.values().stream().mapToLong(l -> l).toArray());
        }

        return checkpointsByIndex;
    }

}
