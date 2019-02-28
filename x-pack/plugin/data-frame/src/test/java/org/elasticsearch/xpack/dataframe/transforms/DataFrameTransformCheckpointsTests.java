/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.TestMatchers.matchesPattern;

public class DataFrameTransformCheckpointsTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformCheckpoints> {

    private static Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(DataFrameTransformsConfigManager.TO_XCONTENT_PARAMS);

    public static DataFrameTransformCheckpoints randomDataFrameTransformCheckpoints() {
        List<Long> checkpoints = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 20); ++i) {
            checkpoints.add(randomNonNegativeLong());
        }

        return new DataFrameTransformCheckpoints(randomAlphaOfLengthBetween(1, 10), checkpoints.stream().mapToLong(l -> l).toArray(),
                randomNonNegativeLong());
    }

    @Override
    protected DataFrameTransformCheckpoints doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpoints.fromXContent(parser, false);
    }

    @Override
    protected DataFrameTransformCheckpoints createTestInstance() {
        return randomDataFrameTransformCheckpoints();
    }

    @Override
    protected Reader<DataFrameTransformCheckpoints> instanceReader() {
        return DataFrameTransformCheckpoints::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    public void testXContentForInternalStorage() throws IOException {
        DataFrameTransformCheckpoints dataFrameTransformCheckpoints = randomDataFrameTransformCheckpoints();

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformCheckpoints.toXContent(xContentBuilder, getToXContentParams());
            String doc = Strings.toString(content);

            assertThat(doc, matchesPattern(".*\"doc_type\"\\s*:\\s*\"data_frame_transforms_checkpoints\".*"));
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformCheckpoints.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String doc = Strings.toString(content);

            assertFalse(doc.contains("doc_type"));
        }
    }
}
