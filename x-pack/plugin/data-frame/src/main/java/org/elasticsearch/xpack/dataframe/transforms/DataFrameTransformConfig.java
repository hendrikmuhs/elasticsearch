/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.dataframe.transforms.pivot.PivotConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the configuration details of a data frame transform
 */
public class DataFrameTransformConfig extends AbstractDiffable<DataFrameTransformConfig> implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transforms";
    private static final ParseField HEADERS = new ParseField("headers");
    private static final ParseField SOURCE = new ParseField("source");
    private static final ParseField DESTINATION = new ParseField("dest");
    private static final ParseField QUERY = new ParseField("query");

    // types of transforms
    private static final ParseField PIVOT_TRANSFORM = new ParseField("pivot");

    private static final ConstructingObjectParser<DataFrameTransformConfig, String> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<DataFrameTransformConfig, String> LENIENT_PARSER = createParser(true);

    private final String id;
    private final String source;
    private final String dest;

    // headers store the user context from the creating user, which allows us to run the transform as this user
    // the header only contains name, groups and other context but no authorization keys
    private Map<String, String> headers;

    private final QueryConfig queryConfig;
    private final PivotConfig pivotConfig;

    private static ConstructingObjectParser<DataFrameTransformConfig, String> createParser(boolean lenient) {
        ConstructingObjectParser<DataFrameTransformConfig, String> parser = new ConstructingObjectParser<>(NAME, lenient,
                (args, optionalId) -> {
                    String id = args[0] != null ? (String) args[0] : optionalId;
                    String source = (String) args[1];
                    String dest = (String) args[2];
                    @SuppressWarnings("unchecked")
                    Map<String, String> headers = (Map<String, String>) args[3];
                    QueryConfig queryConfig = (QueryConfig) args[4];
                    PivotConfig pivotConfig = (PivotConfig) args[5];
                    return new DataFrameTransformConfig(id, source, dest, headers, queryConfig, pivotConfig);
                });

        parser.declareString(optionalConstructorArg(), DataFrameField.ID);
        parser.declareString(constructorArg(), SOURCE);
        parser.declareString(constructorArg(), DESTINATION);

        // NORELEASE injecting the header from outside (e.g. REST) needs to be prevented, store headers in the index?
        // headers are optional, as they can not be set at creation time of a rest call
        // however missing headers cause validation to fail
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), HEADERS);

        parser.declareObject(optionalConstructorArg(), (p, c) -> QueryConfig.fromXContent(p, lenient), QUERY);
        parser.declareObject(optionalConstructorArg(), (p, c) -> PivotConfig.fromXContent(p, lenient), PIVOT_TRANSFORM);

        return parser;
    }

    public static String documentId(String transformId) {
        return "data_frame-" + transformId;
    }

    public DataFrameTransformConfig(final String id,
                                    final String source,
                                    final String dest,
                                    final Map<String, String> headers,
                                    final QueryConfig queryConfig,
                                    final PivotConfig pivotConfig) {
        this.id = ExceptionsHelper.requireNonNull(id, DataFrameField.ID.getPreferredName());
        this.source = ExceptionsHelper.requireNonNull(source, SOURCE.getPreferredName());
        this.dest = ExceptionsHelper.requireNonNull(dest, DESTINATION.getPreferredName());
        this.setHeaders(headers == null ? Collections.emptyMap() : headers);
        this.queryConfig = queryConfig;
        this.pivotConfig = pivotConfig;

        // at least one function must be defined
        if (this.pivotConfig == null) {
            throw new IllegalArgumentException(DataFrameMessages.DATA_FRAME_TRANSFORM_CONFIGURATION_NO_TRANSFORM);
        }
    }

    public DataFrameTransformConfig(final StreamInput in) throws IOException {
        id = in.readString();
        source = in.readString();
        dest = in.readString();
        setHeaders(in.readMap(StreamInput::readString, StreamInput::readString));
        queryConfig = in.readOptionalWriteable(QueryConfig::new);
        pivotConfig = in.readOptionalWriteable(PivotConfig::new);
    }

    public String getId() {
        return id;
    }

    public String getCron() {
        return "*";
    }

    public String getSource() {
        return source;
    }

    public String getDestination() {
        return dest;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public PivotConfig getPivotConfig() {
        return pivotConfig;
    }

    public QueryConfig getQueryConfig() {
        return queryConfig;
    }

    public boolean isValid() {
        /*if (getHeaders().isEmpty()) {
            return false;
        }*/

        // collect validation results from all child objects
        if (queryConfig != null && queryConfig.isValid() == false) {
            return false;
        }

        if (pivotConfig != null && pivotConfig.isValid() == false) {
            return false;
        }

        return true;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(source);
        out.writeString(dest);
        out.writeMap(getHeaders(), StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalWriteable(queryConfig);
        out.writeOptionalWriteable(pivotConfig);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(SOURCE.getPreferredName(), source);
        builder.field(DESTINATION.getPreferredName(), dest);
        if (queryConfig != null) {
            builder.field(QUERY.getPreferredName(), queryConfig);
        }
        if (pivotConfig != null) {
            builder.field(PIVOT_TRANSFORM.getPreferredName(), pivotConfig);
        }
        if (getHeaders().isEmpty() == false && params.paramAsBoolean(DataFrameField.FOR_INTERNAL_STORAGE, false) == true) {
            builder.field(HEADERS.getPreferredName(), getHeaders());
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final DataFrameTransformConfig that = (DataFrameTransformConfig) other;

        return Objects.equals(this.id, that.id)
                && Objects.equals(this.source, that.source)
                && Objects.equals(this.dest, that.dest)
                && Objects.equals(this.getHeaders(), that.getHeaders())
                && Objects.equals(this.queryConfig, that.queryConfig)
                && Objects.equals(this.pivotConfig, that.pivotConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, dest, getHeaders(), queryConfig, pivotConfig);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static DataFrameTransformConfig fromXContent(final XContentParser parser, @Nullable final String optionalTransformId,
            boolean lenient) throws IOException {

        return lenient ? LENIENT_PARSER.apply(parser, optionalTransformId) : STRICT_PARSER.apply(parser, optionalTransformId);
    }
}
