/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.dataframe.transforms.pivot.PivotConfigTests;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DataFrameTransformConfigTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformConfig> {

    private static Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
            Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true"));

    private String transformId;

    public static DataFrameTransformConfig randomDataFrameTransformConfigWithoutHeaders() {
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), null, QueryConfigTests.randomQueryConfig(),
                PivotConfigTests.randomPivotConfig());
    }

    public static DataFrameTransformConfig randomDataFrameTransformConfig() {
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), randomHeaders(), QueryConfigTests.randomQueryConfig(),
                PivotConfigTests.randomPivotConfig());
    }

    public static DataFrameTransformConfig randomInvalidDataFrameTransformConfig() {
        if (randomBoolean()) {
            return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10), randomHeaders(), QueryConfigTests.randomInvalidQueryConfig(),
                    PivotConfigTests.randomPivotConfig());
        } // else
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), randomHeaders(), QueryConfigTests.randomQueryConfig(),
                PivotConfigTests.randomInvalidPivotConfig());
    }

    @Before
    public void setUpOptionalId() {
        transformId = randomAlphaOfLengthBetween(1, 10);

    }

    @Override
    protected DataFrameTransformConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return DataFrameTransformConfig.fromXContent(parser, transformId, false);
        } else {
            return DataFrameTransformConfig.fromXContent(parser, null, false);
        }
    }

    @Override
    protected DataFrameTransformConfig createTestInstance() {
        return randomDataFrameTransformConfig();
    }

    @Override
    protected Reader<DataFrameTransformConfig> instanceReader() {
        return DataFrameTransformConfig::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    private static Map<String, String> randomHeaders() {
        Map<String, String> headers = new HashMap<>(1);
        headers.put("key", "value");

        return headers;
    }
}
