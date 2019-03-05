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
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformCheckpoints extends AbstractDiffable<DataFrameTransformCheckpoints> implements Writeable, ToXContentObject {

    // the timestamp of the checkpoint, mandatory
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    // checkpoint of the indexes (sequence id's)
    public static final ParseField CHECKPOINTS = new ParseField("checkpoints");
    // checkpoint for for time based sync
    public static final ParseField TIMESTAMP_CHECKPOINT = new ParseField("timestamp_checkpoint");

    private static final String NAME = "data_frame_transform_checkpoints";

    private static final ConstructingObjectParser<DataFrameTransformCheckpoints, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<DataFrameTransformCheckpoints, Void> LENIENT_PARSER = createParser(true);

    private final String id;
    private final Map<String, long[]> checkpoints;
    private final long timestamp;
    private final long timestampCheckpoint;

    private static ConstructingObjectParser<DataFrameTransformCheckpoints, Void> createParser(boolean lenient) {
        ConstructingObjectParser<DataFrameTransformCheckpoints, Void> parser = new ConstructingObjectParser<>(NAME,
                lenient, args -> {
                    String id = (String) args[0];
                    Long timestamp = (Long) args[1];

                    @SuppressWarnings("unchecked")
                    Map<String, long[]> checkpoints = (Map<String, long[]>) args[2];

                    Long timestamp_checkpoint = (Long) args[3];

                    // ignored, only for internal storage: String docType = (String) args[4];
                    return new DataFrameTransformCheckpoints(id, timestamp, checkpoints, timestamp_checkpoint);
                });

        parser.declareString(constructorArg(), DataFrameField.ID);
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

                // TODO: type checks
                long[] checkpoints = p.listOrderedMap().stream().mapToLong(num -> ((Number) num).longValue()).toArray();
                checkPointsByIndexName.put(indexName, checkpoints);
            }
            return checkPointsByIndexName;
        }, CHECKPOINTS);
        parser.declareLong(optionalConstructorArg(), TIMESTAMP_CHECKPOINT);
        parser.declareString(optionalConstructorArg(), DataFrameField.INDEX_DOC_TYPE);

        return parser;
    }

    public DataFrameTransformCheckpoints(String id, Long timestamp, Map<String, long[]> checkpoints, Long timestamp_checkpoint) {
        this.id = id;
        this.timestamp = timestamp.longValue();
        this.checkpoints = checkpoints;
        this.timestampCheckpoint = timestamp_checkpoint == null ? 0 : timestamp_checkpoint.longValue();
    }

    public DataFrameTransformCheckpoints(StreamInput in) throws IOException {
        this.id = in.readString();
        this.timestamp = in.readLong();
        this.checkpoints = readCheckpoints(in.readMap());
        this.timestampCheckpoint = in.readLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(TIMESTAMP.getPreferredName(), timestamp);

        builder.startObject(CHECKPOINTS.getPreferredName());
        for (Entry<String, long[]> entry : checkpoints.entrySet()) {
            builder.array(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        if (timestampCheckpoint > 0) {
            builder.field(TIMESTAMP_CHECKPOINT.getPreferredName(), timestampCheckpoint);
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
        out.writeLong(timestamp);
        out.writeGenericValue(checkpoints);
        out.writeLong(timestampCheckpoint);
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

        return Objects.equals(this.id, that.id)
                && this.timestamp == that.timestamp
                && this.checkpoints.size() == that.checkpoints.size() // quick check
                && this.timestampCheckpoint == that.timestampCheckpoint
                // do the expensive deep equal operation last
                && this.checkpoints.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), that.checkpoints.get(e.getKey())));
    }

    @Override
    public int hashCode() {
        int hash = Objects.hash(id, timestamp, timestampCheckpoint);

        for (Entry<String, long[]> e : checkpoints.entrySet()) {
            hash = 31 * hash + Objects.hash(e.getKey(), Arrays.hashCode(e.getValue()));
        }
        return hash;
    }

    public static DataFrameTransformCheckpoints fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public static String documentId(String transformId) {
        return NAME + "-" + transformId;
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
