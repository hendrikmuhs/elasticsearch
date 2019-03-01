/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformCheckpoints extends AbstractDiffable<DataFrameTransformCheckpoints> implements Writeable, ToXContentObject {

    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField CHECKPOINTS = new ParseField("checkpoints");

    private static final String NAME = "data_frame_transform_checkpoints";

    private static final ConstructingObjectParser<DataFrameTransformCheckpoints, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<DataFrameTransformCheckpoints, Void> LENIENT_PARSER = createParser(true);

    private final String id;
    private final long[] checkpoints;
    private final long timestamp;

    private static ConstructingObjectParser<DataFrameTransformCheckpoints, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DataFrameTransformCheckpoints, Void> parser = new ConstructingObjectParser<>(NAME,
                lenient, args -> {
                    String id = (String) args[0];
                    @SuppressWarnings("unchecked")
                    List<Long> checkpoints = (List<Long>) args[1];
                    long timestamp = (long) args[2];

                    // ignored, only for internal storage: String docType = (String) args[3];

                    return new DataFrameTransformCheckpoints(id, checkpoints.stream().mapToLong(l -> l).toArray(), timestamp);
                });

        parser.declareString(constructorArg(), DataFrameField.ID);
        parser.declareLongArray(constructorArg(), CHECKPOINTS);
        parser.declareLong(optionalConstructorArg(), TIMESTAMP);
        parser.declareString(optionalConstructorArg(), DataFrameField.INDEX_DOC_TYPE);

        return parser;
    }

    public DataFrameTransformCheckpoints(String id, long[] checkpoints, long timestamp) {
        this.id = id;
        this.checkpoints = checkpoints;
        this.timestamp = timestamp;
    }

    public DataFrameTransformCheckpoints(StreamInput in) throws IOException {
        this.id = in.readString();
        this.checkpoints = in.readLongArray();
        this.timestamp = in.readLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.array(CHECKPOINTS.getPreferredName(), checkpoints);
        if (timestamp > 0) {
            builder.field(TIMESTAMP.getPreferredName(), timestamp);
        }
        if (params.paramAsBoolean(DataFrameField.INCLUDE_TYPE, false)) {
            builder.field(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), NAME);
        }
        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLongArray(checkpoints);
        out.writeLong(timestamp);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final DataFrameTransformCheckpoints that = (DataFrameTransformCheckpoints) other;

        return Objects.equals(this.id, that.id) && Arrays.equals(this.checkpoints, that.checkpoints) && this.timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, Arrays.hashCode(checkpoints), timestamp);
    }

    public static DataFrameTransformCheckpoints fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static String documentId(String transformId) {
        return NAME + "-" + transformId;
    }
}
