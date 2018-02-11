/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline.correl;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;

/*

parse DSL queries

 */
public class CorrelationPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<CorrelationPipelineAggregationBuilder> {
    public static final String NAME = "correl";

    private final Map<String, String> bucketsPathsMap;
    private static final ParseField GAP_POLICY = new ParseField("gap_policy");
    private static final ParseField LAG = new ParseField("lag");

    private String format;
    private GapPolicy gapPolicy = GapPolicy.SKIP;
    private int lag = 1;

    public CorrelationPipelineAggregationBuilder(String name, Map<String, String> bucketsPathsMap, int lag) {
        super(name, NAME, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.lag=lag;
    }

    public CorrelationPipelineAggregationBuilder(String name, int lag, String... bucketsPaths) {
        this(name, convertToBucketsPathMap(bucketsPaths), lag);
    }

    /**
     * Read from a stream.
     */
    public CorrelationPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);


        int mapSize = in.readVInt();
        bucketsPathsMap = new HashMap<String, String>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            bucketsPathsMap.put(in.readString(), in.readString());
        }
        lag = in.readVInt();
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);


    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(bucketsPathsMap.size());
        for (Map.Entry<String, String> e : bucketsPathsMap.entrySet()) {
            out.writeString(e.getKey());
            out.writeString(e.getValue());
        }
        out.writeVInt(lag);
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);

    }

    /**
     * Sets the lag to use when calculating the serial difference.
     */
    public CorrelationPipelineAggregationBuilder lag(int lag) {
        if (lag <= 0) {
            throw new IllegalArgumentException("[lag] must be a positive integer: [" + name + "]");
        }
        this.lag = lag;
        return this;
    }

    /**
     * Gets the lag to use when calculating the serial difference.
     */
    public int lag() {
        return lag;
    }

    /**
     * Sets the format to use on the output of this aggregation.
     */
    public CorrelationPipelineAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    /**
     * Gets the format to use on the output of this aggregation.
     */
    public String format() {
        return format;
    }

    /**
     * Sets the GapPolicy to use on the output of this aggregation.
     */
    public CorrelationPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[gapPolicy] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    /**
     * Gets the GapPolicy to use on the output of this aggregation.
     */
    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new CorrelationPipelineAggregator(name, bucketsPathsMap, lag, formatter(), gapPolicy, metaData);
    }

    //buckets_path is not serialized
    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field(BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
        builder.field(LAG.getPreferredName(), lag);
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy.getName());

        return builder;
    }

    public static CorrelationPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {

        XContentParser.Token token;
        String currentFieldName = null;
        Map<String, String> bucketsPathsMap = null;
        String format = null;
        GapPolicy gapPolicy = null;
        Integer lag = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (FORMAT.match(currentFieldName)) {
                    format = parser.text();
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    bucketsPathsMap = new HashMap<>();
                    bucketsPathsMap.put("_value", parser.text());
                } else if (GAP_POLICY.match(currentFieldName)) {
                    gapPolicy = GapPolicy.parse(parser.text(), parser.getTokenLocation());
                } else if (LAG.match(currentFieldName)) {
                    lag = parser.intValue(true);
                    if (lag <= 0) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "Lag must be a positive, non-zero integer.  Value supplied was" +
                                        lag + " in [" + reducerName + "]: ["
                                        + currentFieldName + "].");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPathsMap = new HashMap<>();
                    for (int i = 0; i < paths.size(); i++) {
                        bucketsPathsMap.put("_value" + i, paths.get(i));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (LAG.match(currentFieldName)) {
                    lag = parser.intValue(true);
                    if (lag <= 0) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "Lag must be a positive, non-zero integer.  Value supplied was" +
                                        lag + " in [" + reducerName + "]: ["
                                        + currentFieldName + "].");
                    }
                } else if (BUCKETS_PATH.match(currentFieldName)) {
                    Map<String, Object> map = parser.map();
                    bucketsPathsMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        bucketsPathsMap.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (LAG.match(currentFieldName)) {
                    lag = parser.intValue(true);
                    if (lag <= 0) {
                        throw new ParsingException(parser.getTokenLocation(),
                                "Lag must be a positive, non-zero integer.  Value supplied was" +
                                        lag + " in [" + reducerName + "]: ["
                                        + currentFieldName + "].");
                    }
                }  else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        if (bucketsPathsMap == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for series_arithmetic aggregation [" + reducerName + "]");
        }

        if (lag == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + LAG.getPreferredName()
                    + "] for series_arithmetic aggregation [" + reducerName + "]");
        }



        CorrelationPipelineAggregationBuilder factory =
                new CorrelationPipelineAggregationBuilder(reducerName, bucketsPathsMap,lag);
        if (lag != null) {
            factory.lag(lag);
        }
        if (format != null) {
            factory.format(format);
        }
        if (gapPolicy != null) {
            factory.gapPolicy(gapPolicy);
        }
        return factory;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(bucketsPathsMap, lag, format, gapPolicy);
    }
    @Override
    protected boolean doEquals(Object obj) {
        CorrelationPipelineAggregationBuilder other = (CorrelationPipelineAggregationBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap) && Objects.equals(lag, other.lag)
                && Objects.equals(format, other.format) && Objects.equals(gapPolicy, other.gapPolicy);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private static Map<String, String> convertToBucketsPathMap(String[] bucketsPaths) {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        for (int i = 0; i < bucketsPaths.length; i++) {
            bucketsPathsMap.put("_value" + i, bucketsPaths[i]);
        }
        return bucketsPathsMap;
    }
}
