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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.correl.CorrelationPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregationBuilder;

import java.util.HashMap;
import java.util.Map;

public class CorrelTests extends BasePipelineAggregationTestCase<CorrelationPipelineAggregationBuilder> {

    @Override
    protected CorrelationPipelineAggregationBuilder createTestAggregatorFactory() {
        String name = randomAlphaOfLengthBetween(3, 20);

        Map<String, String> bucketsPaths = new HashMap<>();
        int numBucketPaths = 2;
        for (int i = 0; i < numBucketPaths; i++) {
            bucketsPaths.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 40));
        }

        CorrelationPipelineAggregationBuilder factory = new CorrelationPipelineAggregationBuilder(name,bucketsPaths,1);
        if (randomBoolean()) {
            factory.format(randomAlphaOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(GapPolicy.values()));
        }
        if (randomBoolean()) {
            factory.lag(randomIntBetween(1, 1000));
        }
        return factory;
    }

}
