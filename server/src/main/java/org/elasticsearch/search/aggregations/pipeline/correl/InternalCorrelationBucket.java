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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;

public class InternalCorrelationBucket extends InternalNumericMetricsAggregation.MultiValue  {

    private  String[] leftKeys;
    private  String[] rightKeys;
    private double[] correlationValues;
    public static final String NAME = "correlation_bucket";

    public InternalCorrelationBucket(String name, String[] leftKeys, String[] rightKeys, double[] correlationValues , DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metadata) {
        super(name, pipelineAggregators, metadata);
        if (CollectionUtils.isEmpty(leftKeys)||CollectionUtils.isEmpty(rightKeys)||null==correlationValues||leftKeys.length!=rightKeys.length||leftKeys.length!=correlationValues.length) {
            throw new IllegalArgumentException("The correlation is non-existent");
        }
        this.format = formatter;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.correlationValues = correlationValues;
    }



    /**
     * Read from a stream.
     */
    public InternalCorrelationBucket(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        leftKeys = in.readStringArray();
        rightKeys = in.readStringArray();
        correlationValues = in.readDoubleArray();
    }


    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeStringArray(leftKeys);
        out.writeStringArray(rightKeys);
        out.writeDoubleArray(correlationValues);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    DocValueFormat formatter() {
        return format;
    }


    @Override
    public double value(String name) {
        return (Double.parseDouble(name));
    }

    @Override
    public InternalMax doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("correlations");
        for (int i=0;i<correlationValues.length;i++) {
            builder.startObject();
            double value = correlationValues[i];
            boolean hasValue = !(Double.isInfinite(value) || Double.isNaN(value));
            String leftKey = leftKeys[i];
            String rightKey = rightKeys[i];
            builder.field("correlation", hasValue ? value : null);
            builder.field("first_path", leftKey);
            builder.field("second_path", rightKey);

            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalCorrelationBucket that = (InternalCorrelationBucket) obj;
        return Arrays.equals(leftKeys, that.leftKeys) && Arrays.equals(rightKeys, that.rightKeys)&& Arrays.equals(correlationValues, that.correlationValues);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(Arrays.hashCode(leftKeys), Arrays.hashCode(rightKeys),Arrays.hashCode(correlationValues));
    }

    public String[] getLeftKeys() {
        return leftKeys;
    }

    public String[] getRightKeys() {
        return rightKeys;
    }

    public double[] getCorrelationValues() {
        return correlationValues;
    }
}
