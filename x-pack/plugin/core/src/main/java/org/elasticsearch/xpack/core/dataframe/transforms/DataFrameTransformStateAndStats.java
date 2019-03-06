/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransformStateAndStats implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transform_state_and_stats";
    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField IN_PROGRESS_CHECKPOINT_FIELD = new ParseField("in_progress_checkpoint");
    public static final ParseField IN_SYNC_FIELD = new ParseField("in_sync");

    private final String id;
    private final DataFrameTransformState transformState;
    private final DataFrameIndexerTransformStats transformStats;
    private final DataFrameTransformCheckpoint inProgressCheckpoint;
    private final boolean inSync;

    public static final ConstructingObjectParser<DataFrameTransformStateAndStats, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new DataFrameTransformStateAndStats((String) a[0], (DataFrameTransformState) a[1], (DataFrameIndexerTransformStats) a[2],
                    (boolean) a[3], (DataFrameTransformCheckpoint) a[4]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DataFrameTransformState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p),
                DataFrameField.STATS_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), IN_SYNC_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> DataFrameTransformCheckpoint.fromXContent(p, true),
                IN_PROGRESS_CHECKPOINT_FIELD);
    }

    public DataFrameTransformStateAndStats(String id, DataFrameTransformState state, DataFrameIndexerTransformStats stats,
            boolean inSync, DataFrameTransformCheckpoint inProgressCheckpoint) {
        this.id = Objects.requireNonNull(id);
        this.transformState = Objects.requireNonNull(state);
        this.transformStats = Objects.requireNonNull(stats);
        this.inSync = inSync;
        this.inProgressCheckpoint = inProgressCheckpoint;
    }

    public DataFrameTransformStateAndStats(StreamInput in) throws IOException {
        this.id = in.readString();
        this.transformState = new DataFrameTransformState(in);
        this.transformStats = new DataFrameIndexerTransformStats(in);
        this.inSync = in.readBoolean();
        if (in.readBoolean()) {
            this.inProgressCheckpoint = new DataFrameTransformCheckpoint(in);
        } else {
            this.inProgressCheckpoint = null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), transformState);
        builder.field(DataFrameField.STATS_FIELD.getPreferredName(), transformStats);
        builder.field(IN_SYNC_FIELD.getPreferredName(), inSync);
        if (inProgressCheckpoint != null) {
            builder.field(IN_PROGRESS_CHECKPOINT_FIELD.getPreferredName(), inProgressCheckpoint);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        transformState.writeTo(out);
        transformStats.writeTo(out);
        out.writeBoolean(inSync);
        if (inProgressCheckpoint != null) {
            out.writeBoolean(true);
            inProgressCheckpoint.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, transformState, transformStats, inSync, inProgressCheckpoint);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformStateAndStats that = (DataFrameTransformStateAndStats) other;

        return Objects.equals(this.id, that.id) && Objects.equals(this.transformState, that.transformState)
                && Objects.equals(this.transformStats, that.transformStats)
                && this.inSync == that.inSync
                && Objects.equals(this.inProgressCheckpoint, that.inProgressCheckpoint);
    }

    public String getId() {
        return id;
    }

    public DataFrameIndexerTransformStats getTransformStats() {
        return transformStats;
    }

    public DataFrameTransformState getTransformState() {
        return transformState;
    }
}
