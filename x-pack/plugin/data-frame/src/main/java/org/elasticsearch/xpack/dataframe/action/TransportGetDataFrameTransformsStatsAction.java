/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.dataframe.persistence.DataFramePersistentTaskUtils;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsCheckpointService;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TransportGetDataFrameTransformsStatsAction extends
        TransportTasksAction<DataFrameTransformTask,
        GetDataFrameTransformsStatsAction.Request,
        GetDataFrameTransformsStatsAction.Response,
        GetDataFrameTransformsStatsAction.Response> {

    private final DataFrameTransformsConfigManager transformsConfigManager;
    private final DataFrameTransformsCheckpointService transformsCheckpointService;

    @Inject
    public TransportGetDataFrameTransformsStatsAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService, DataFrameTransformsConfigManager transformsConfigManager,
            DataFrameTransformsCheckpointService transformsCheckpointService) {
        super(GetDataFrameTransformsStatsAction.NAME, clusterService, transportService, actionFilters, Request::new, Response::new,
                Response::new, ThreadPool.Names.SAME);
        this.transformsConfigManager = transformsConfigManager;
        this.transformsCheckpointService = transformsCheckpointService;
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        List<DataFrameTransformStateAndStats> responses = tasks.stream()
            .flatMap(r -> r.getTransformsStateAndStats().stream())
            .sorted(Comparator.comparing(DataFrameTransformStateAndStats::getId))
            .collect(Collectors.toList());
        return new Response(responses, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask task, ActionListener<Response> listener) {
        assert task.getTransformId().equals(request.getId()) || request.getId().equals(MetaData.ALL);

        // Little extra insurance, make sure we only return transforms that aren't cancelled
        if (task.isCancelled() == false) {
            CountDownLatch latch = new CountDownLatch(2);

            // the checkpoints in progress
            SetOnce<DataFrameTransformCheckpoint> inProgressCheckpoint = new SetOnce<>();
            SetOnce<DataFrameTransformCheckpoint> currentCheckpoint = new SetOnce<>();

            transformsConfigManager.getTransformCheckpoints(task.getTransformId(),
                    new LatchedActionListener<>(ActionListener.wrap(checkpoints -> {
                        inProgressCheckpoint.set(checkpoints);
                    }, e -> {
                        // todo: log
                    }), latch));

            transformsConfigManager.getTransformConfiguration(task.getTransformId(),
                    new LatchedActionListener<>(ActionListener.wrap(transformConfig -> {
                        transformsCheckpointService.getCheckpoint(transformConfig, ActionListener.wrap(checkpoint -> {
                            currentCheckpoint.set(checkpoint);
                        }, e2 -> {
                        }));

                    }, e -> {
                    }), latch));

            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }

            boolean checkpointsMatch = false;

            if (currentCheckpoint.get() != null && inProgressCheckpoint.get() != null) {
                checkpointsMatch = currentCheckpoint.get().matches(inProgressCheckpoint.get());
            }

            DataFrameTransformStateAndStats transformStateAndStats = new DataFrameTransformStateAndStats(task.getTransformId(),
                    task.getState(), task.getStats(), checkpointsMatch, inProgressCheckpoint.get());
            listener.onResponse(new Response(Collections.singletonList(transformStateAndStats)));
        } else {
            listener.onResponse(new Response(Collections.emptyList()));
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster()) {
            if (DataFramePersistentTaskUtils.stateHasDataFrameTransforms(request.getId(), state)) {
                super.doExecute(task, request, listener);
            } else {
                // If we couldn't find the transform in the persistent task CS, it means it was deleted prior to this GET
                // and we can just send an empty response, no need to go looking for the allocated task
                listener.onResponse(new Response(Collections.emptyList()));
            }

        } else {
            // Delegates GetTransforms to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows transforms which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master nodes"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, Response::new));
            }
        }
    }
}
