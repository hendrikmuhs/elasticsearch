/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformCheckpoint extends AbstractDiffable<DataFrameTransformCheckpoint> implements Writeable, ToXContentObject {

    // the timestamp of the checkpoint, mandatory
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    // the own checkpoint
    public static final ParseField CHECKPOINT = new ParseField("checkpoint");

    // checkpoint of the indexes (sequence id's)
    public static final ParseField INDICES = new ParseField("indices");

    // checkpoint for for time based sync
    // TODO: consider a lower bound for usecases where you want to transform on a window of a stream
    public static final ParseField TIME_UPPER_BOUND = new ParseField("time_upper_bound");

    private static final String NAME = "data_frame_transform_checkpoint";

    private static final ConstructingObjectParser<DataFrameTransformCheckpoint, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<DataFrameTransformCheckpoint, Void> LENIENT_PARSER = createParser(true);

    private final String id;
    private final long timestampMillis;
    private final long checkpoint;
    private final Map<String, long[]> indicesCheckpoints;
    private final long timeUpperBoundMillis;

    private static ConstructingObjectParser<DataFrameTransformCheckpoint, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DataFrameTransformCheckpoint, Void> parser = new ConstructingObjectParser<>(NAME,
                lenient, args -> {
                    String id = (String) args[0];
                    Long timestamp = (Long) args[1];
                    Long checkpoint = (Long) args[2];

                    @SuppressWarnings("unchecked")
                    Map<String, long[]> checkpoints = (Map<String, long[]>) args[3];

                    Long timestamp_checkpoint = (Long) args[4];

                    // ignored, only for internal storage: String docType = (String) args[5];
                    return new DataFrameTransformCheckpoint(id, timestamp, checkpoint, checkpoints, timestamp_checkpoint);
                });

        parser.declareString(constructorArg(), DataFrameField.ID);

        // note: this is never parsed from the outside where timestamp can be formatted as date time
        parser.declareLong(constructorArg(), TIMESTAMP);

        parser.declareObject(constructorArg(), (p,c) -> {
            Map<String, long[]> checkPointsByIndexName = new TreeMap<>();
            XContentParser.Token token = null;
            while ((token = p.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(p.getTokenLocation(), "Unexpected token " + token + " ");
                }

                final String indexName = p.currentName();
                token = p.nextToken();
                if (token != XContentParser.Token.START_ARRAY) {
                    throw new ParsingException(p.getTokenLocation(), "Unexpected token " + token + " ");
                }

                long[] checkpoints = p.listOrderedMap().stream().mapToLong(num -> ((Number) num).longValue()).toArray();
                checkPointsByIndexName.put(indexName, checkpoints);
            }
            return checkPointsByIndexName;
        }, INDICES);
        parser.declareLong(optionalConstructorArg(), TIME_UPPER_BOUND);
        parser.declareString(optionalConstructorArg(), DataFrameField.INDEX_DOC_TYPE);

        return parser;
    }

    public DataFrameTransformCheckpoint(String id, Long timestamp, Long checkpoint, Map<String, long[]> checkpoints, Long timeUpperBound) {
        this.id = id;
        this.timestampMillis = timestamp.longValue();
        this.checkpoint = checkpoint;
        this.indicesCheckpoints = checkpoints;
        this.timeUpperBoundMillis = timeUpperBound == null ? 0 : timeUpperBound.longValue();
    }

    public DataFrameTransformCheckpoint(StreamInput in) throws IOException {
        this.id = in.readString();
        this.timestampMillis = in.readLong();
        this.checkpoint = in.readLong();
        this.indicesCheckpoints = readCheckpoints(in.readMap());
        this.timeUpperBoundMillis = in.readLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final String humanReadableSuffix = "_string";
        builder.startObject();

        // the id, doc_type and checkpoint is only internally used for storage, the user-facing version gets embedded
        if (params.paramAsBoolean(DataFrameField.FOR_INTERNAL_STORAGE, false)) {
            builder.field(DataFrameField.ID.getPreferredName(), id);
            builder.field(CHECKPOINT.getPreferredName(), checkpoint);
            builder.field(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), NAME);
        }

        builder.field(TIMESTAMP.getPreferredName(), timestampMillis);
        if (timeUpperBoundMillis > 0) {
            builder.field(TIME_UPPER_BOUND.getPreferredName(), timeUpperBoundMillis);
        }

        if (params.paramAsBoolean("human", false)) {
            // be human readable on API level
            builder.field(TIMESTAMP.getPreferredName() + humanReadableSuffix, Instant.ofEpochMilli(timestampMillis).atZone(ZoneOffset.UTC));
            if (timeUpperBoundMillis > 0) {
                builder.field(TIME_UPPER_BOUND.getPreferredName() + humanReadableSuffix,
                        Instant.ofEpochMilli(timeUpperBoundMillis).atZone(ZoneOffset.UTC));
            }
        }

        builder.startObject(INDICES.getPreferredName());
        for (Entry<String, long[]> entry : indicesCheckpoints.entrySet()) {
            builder.array(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestampMillis;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public Map<String, long[]> getIndicesCheckpoints() {
        return indicesCheckpoints;
    }

    public long getTimeUpperBound() {
        return timeUpperBoundMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(timestampMillis);
        out.writeLong(checkpoint);
        out.writeGenericValue(indicesCheckpoints);
        out.writeLong(timeUpperBoundMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final DataFrameTransformCheckpoint that = (DataFrameTransformCheckpoint) other;

        return this.timestampMillis == that.timestampMillis && this.checkpoint == that.checkpoint && matches(that);
    }

    public boolean matches (DataFrameTransformCheckpoint that) {
        if (this == that) {
            return true;
        }

        return Objects.equals(this.id, that.id)
                && this.indicesCheckpoints.size() == that.indicesCheckpoints.size() // quick check
                && this.timeUpperBoundMillis == that.timeUpperBoundMillis
                // do the expensive deep equal operation last
                && this.indicesCheckpoints.entrySet().stream()
                        .allMatch(e -> Arrays.equals(e.getValue(), that.indicesCheckpoints.get(e.getKey())));
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(id, timestampMillis, checkpoint, timeUpperBoundMillis);

        for (Entry<String, long[]> e : indicesCheckpoints.entrySet()) {
            hash = 31 * hash + Objects.hash(e.getKey(), Arrays.hashCode(e.getValue()));
        }
        return hash;
    }

    public static DataFrameTransformCheckpoint fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static String documentId(String transformId, long checkpointId) {
        return NAME + "-" + transformId + "-" + checkpointId;
    }

    private static Map<String, long[]> readCheckpoints(Map<String, Object> readMap) {
        Map<String, long[]> checkpoints = new TreeMap<>();
        for (Map.Entry<String, Object> e : readMap.entrySet()) {
            if (e.getValue() instanceof long[]) {
                checkpoints.put(e.getKey(), (long[]) e.getValue());
            } else {
                throw new ElasticsearchParseException("expecting the checkpoints for [{}] to be a long[], but found [{}] instead",
                        e.getKey(), e.getValue().getClass());
            }
        }
        return checkpoints;
    }
}
