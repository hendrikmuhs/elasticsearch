/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DataFrameTransformsConfigManager {

    private static final Logger logger = LogManager.getLogger(DataFrameTransformsConfigManager.class);

    public static final Map<String, String> TO_XCONTENT_PARAMS = Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true");

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public DataFrameTransformsConfigManager(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    public void putTransformCheckpoint(DataFrameTransformCheckpoint checkpoint, ActionListener<Boolean> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = checkpoint.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(DataFrameInternalIndex.INDEX_NAME)
                    .opType(DocWriteRequest.OpType.INDEX)
                    .id(DataFrameTransformCheckpoint.documentId(checkpoint.getId(), checkpoint.getCheckpoint()))
                    .source(source);

            executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(r -> {
                listener.onResponse(true);
            }, listener::onFailure));
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(e);
        }
    }

    public void putTransformConfiguration(DataFrameTransformConfig transformConfig, ActionListener<Boolean> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = transformConfig.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));

            IndexRequest indexRequest = new IndexRequest(DataFrameInternalIndex.INDEX_NAME)
                    .opType(DocWriteRequest.OpType.CREATE)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .id(DataFrameTransformConfig.documentId(transformConfig.getId()))
                    .source(source);

            executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(r -> {
                listener.onResponse(true);
            }, e -> {
                if (e instanceof VersionConflictEngineException) {
                    // the transform already exists
                    listener.onFailure(new ResourceAlreadyExistsException(
                            DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS,
                                    transformConfig.getId())));
                } else {
                    listener.onFailure(
                            new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_PERSIST_TRANSFORM_CONFIGURATION, e));
                }
            }));
        } catch (IOException e) {
            // not expected to happen but for the sake of completeness
            listener.onFailure(new ElasticsearchParseException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_FAILED_TO_SERIALIZE_TRANSFORM, transformConfig.getId()),
                    e));
        }
    }

    public void getTransformCheckpoint(String transformId, long checkpointId, ActionListener<DataFrameTransformCheckpoint> resultListener) {
        GetRequest getRequest = new GetRequest(DataFrameInternalIndex.INDEX_NAME,
                DataFrameTransformCheckpoint.documentId(transformId, checkpointId));
        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {

            if (getResponse.isExists() == false) {
                // do not fail if checkpoint does not exist but return null
                resultListener.onResponse(null);
                return;
            }
            BytesReference source = getResponse.getSourceAsBytesRef();
            parseCheckpointsLenientlyFromSource(source, transformId, resultListener);
        }, resultListener::onFailure));
    }

    public void getTransformConfiguration(String transformId, ActionListener<DataFrameTransformConfig> resultListener) {
        GetRequest getRequest = new GetRequest(DataFrameInternalIndex.INDEX_NAME, DataFrameTransformConfig.documentId(transformId));
        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {

            if (getResponse.isExists() == false) {
                resultListener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
                return;
            }
            BytesReference source = getResponse.getSourceAsBytesRef();
            parseTransformLenientlyFromSource(source, transformId, resultListener);
        }, e -> {
            if (e.getClass() == IndexNotFoundException.class) {
                resultListener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
            } else {
                resultListener.onFailure(e);
            }
        }));
    }

    public void deleteTransform(String transformId, ActionListener<Boolean> listener) {

        DeleteByQueryRequest request = new DeleteByQueryRequest()
                .setAbortOnVersionConflict(false) //since these documents are not updated, a conflict just means it was deleted previously
                .setSlices(5);

        request.indices(DataFrameInternalIndex.INDEX_NAME);
        QueryBuilder query = QueryBuilders.termsQuery(DataFrameField.ID.getPreferredName(), transformId);
        request.setQuery(query);
        request.setRefresh(true);

        executeAsyncWithOrigin(client, DATA_FRAME_ORIGIN, DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(deleteResponse -> {

            if (deleteResponse.getDeleted() == 0) {
                listener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
                return;
            }
            listener.onResponse(true);
        }, e -> {
            if (e.getClass() == IndexNotFoundException.class) {
                listener.onFailure(new ResourceNotFoundException(
                        DataFrameMessages.getMessage(DataFrameMessages.REST_DATA_FRAME_UNKNOWN_TRANSFORM, transformId)));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void parseTransformLenientlyFromSource(BytesReference source, String transformId,
            ActionListener<DataFrameTransformConfig> transformListener) {
        try (InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            transformListener.onResponse(DataFrameTransformConfig.fromXContent(parser, transformId, true));
        } catch (Exception e) {
            logger.error(DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_PARSE_TRANSFORM_CONFIGURATION, transformId), e);
            transformListener.onFailure(e);
        }
    }

    private void parseCheckpointsLenientlyFromSource(BytesReference source, String transformId,
            ActionListener<DataFrameTransformCheckpoint> transformListener) {
        try (InputStream stream = source.streamInput();
                XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)) {
            transformListener.onResponse(DataFrameTransformCheckpoint.fromXContent(parser, true));
        } catch (Exception e) {
            logger.error(DataFrameMessages.getMessage(DataFrameMessages.FAILED_TO_PARSE_TRANSFORM_CHECKPOINTS, transformId), e);
            transformListener.onFailure(e);
        }
    }
}
