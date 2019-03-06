/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.test.TestMatchers.matchesPattern;

public class DataFrameTransformCheckpointTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformCheckpoint> {

    private static final Params TO_XCONTENT_PARAMS;

    static {
        Map<String, String> mapParams = new HashMap<>();
        mapParams.put(DataFrameField.FOR_INTERNAL_STORAGE, "true");
        mapParams.put(DataFrameField.INCLUDE_TYPE, "true");
        TO_XCONTENT_PARAMS = new ToXContent.MapParams(mapParams);
    }

    public static DataFrameTransformCheckpoint randomDataFrameTransformCheckpoints() {

        Map<String, long[]> checkpointsByIndex = new TreeMap<>();
        for (int i = 0; i < randomIntBetween(1, 10); ++i) {
            List<Long> checkpoints = new ArrayList<>();
            for (int j = 0; j < randomIntBetween(1, 20); ++j) {
                checkpoints.add(randomNonNegativeLong());
            }
            checkpointsByIndex.put(randomAlphaOfLengthBetween(1, 10), checkpoints.stream().mapToLong(l -> l).toArray());
        }
        return new DataFrameTransformCheckpoint(randomAlphaOfLengthBetween(1, 10), randomNonNegativeLong(), checkpointsByIndex,
                randomNonNegativeLong());
    }

    @Override
    protected DataFrameTransformCheckpoint doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpoint.fromXContent(parser, false);
    }

    @Override
    protected DataFrameTransformCheckpoint createTestInstance() {
        return randomDataFrameTransformCheckpoints();
    }

    @Override
    protected Reader<DataFrameTransformCheckpoint> instanceReader() {
        return DataFrameTransformCheckpoint::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    public void testXContentForInternalStorage() throws IOException {
        DataFrameTransformCheckpoint dataFrameTransformCheckpoints = randomDataFrameTransformCheckpoints();

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformCheckpoints.toXContent(xContentBuilder, getToXContentParams());
            String doc = Strings.toString(content);

            assertThat(doc, matchesPattern(".*\"doc_type\"\\s*:\\s*\"data_frame_transform_checkpoints\".*"));
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformCheckpoints.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String doc = Strings.toString(content);

            assertFalse(doc.contains("doc_type"));
        }
    }
}
