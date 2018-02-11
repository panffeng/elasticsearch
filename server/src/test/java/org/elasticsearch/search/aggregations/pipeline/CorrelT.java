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

import com.google.common.collect.Lists;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.pipeline.correl.InternalCorrelationBucket;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.correl;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class CorrelT extends ESIntegTestCase {

    private static final String INDEX = "correlation-it-data-index";
    private static final String INDEX_WITH_GAPS = "correlation-it-data-index-with-gaps";

    private static final String TIME_FIELD = "time";
    private static final String TERM_1_FIELD = "foo";
    private static final String TERM_2_FIELD = "bar";
    private static final String VALUE_FIELD = "value";
    private String barValue;


    @Override
    public void setupSuiteScopeCluster() throws Exception {

        createIndex(INDEX, INDEX_WITH_GAPS);
        client().admin().indices().preparePutMapping(INDEX)
                .setType("doc")
                .setSource(TIME_FIELD, "type=date", TERM_1_FIELD, "type=keyword", TERM_2_FIELD, "type=keyword", VALUE_FIELD, "type=float")
                .get();

        int numFooValue = randomIntBetween(1, 10);
        List<String> fooValues = new ArrayList<>(numFooValue);
        for (int i = 0; i < numFooValue; ++i) {
            fooValues.add(randomAlphaOfLengthBetween(3, 8));
        }

        int numBarValue = 2;
        List<String> barValues = new ArrayList<>(numBarValue);
        for (int i = 0; i < numBarValue; ++i) {
            barValues.add(randomAlphaOfLengthBetween(3, 8));
        }

        long now = System.currentTimeMillis();
        long time = now - TimeValue.timeValueHours(240).millis();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        while (time < now) {
            int termCount = randomIntBetween(1, 6);
            for (int i = 0; i < termCount; ++i) {
                builders.add(client().prepareIndex(INDEX, "doc")
                        .setSource(newDocBuilder(time, fooValues.get(randomInt(numFooValue-1)), barValues.get(randomInt(numBarValue-1)), randomIntBetween(1, 100) * randomDouble())));
            }

            time += TimeValue.timeValueHours(1).millis();
        }

        builders.add(client().prepareIndex(INDEX_WITH_GAPS, "doc").setSource(newDocBuilder(1, "foo", "bar", 42.0)));
        builders.add(client().prepareIndex(INDEX_WITH_GAPS, "doc").setSource(newDocBuilder(2, "foo", null, 42.0)));
        builders.add(client().prepareIndex(INDEX_WITH_GAPS, "doc").setSource(newDocBuilder(3, "foo", "bar", 42.0)));

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder(long timeMillis, String fooValue, String barValue, Double value) throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(TIME_FIELD, timeMillis);

        if (fooValue != null) {
            jsonBuilder.field(TERM_1_FIELD, fooValue);
        }
        if (barValue != null) {
            jsonBuilder.field(TERM_2_FIELD, barValue);
        }

        jsonBuilder.field(VALUE_FIELD, value);

        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testOnSiblingAggregation() {

        SearchResponse response0 = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(terms("foos").field(TERM_2_FIELD))
                .execute().actionGet();

        assertSearchResponse(response0);

        Terms terms = response0.getAggregations().get("foos");



        Terms.Bucket bucket= terms.getBuckets().get(0);
        String term2Value= bucket.getKeyAsString();

        String firstPath = "time_buckets>filter>sum_partial";
        String secondPath = "time_buckets>overall";

        // Only sibling pipeline aggregations are allowed at the top level
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX)
                .setSize(0)
                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis())
                        .subAggregation(sum("overall").field(VALUE_FIELD))
                        .subAggregation(filter("filter", termQuery(TERM_2_FIELD, term2Value))
                                .subAggregation(sum("sum_partial").field(VALUE_FIELD))))
                .addAggregation(correl("correlation", 1, firstPath, secondPath)
                );
        ;

//        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX)
//                .setSize(0)
//                .addAggregation(dateHistogram("time_buckets").field(TIME_FIELD).interval(TimeValue.timeValueHours(1).millis())
//                        .subAggregation(sum("overall").field(VALUE_FIELD))
//                        .subAggregation(filter("filter", termQuery(TERM_2_FIELD, term2Value))
//                                .subAggregation(sum("sum_partial").field(VALUE_FIELD)))
//                        .subAggregation(correl("correlation", 1, "filter>sum_partial", "overall"))
//                );
        SearchResponse response = searchRequestBuilder.execute().actionGet();

        String query = searchRequestBuilder.toString();
        System.out.println(query);
        assertSearchResponse(response);
        System.out.println(response);

        Aggregation correlation = response.getAggregations().get("correlation");

        System.out.println("correlation");
        System.out.println(correlation);

        assertThat(correlation, notNullValue());
        assertThat(correlation.getName(), equalTo("correlation"));

        InternalCorrelationBucket correl_bucket = (InternalCorrelationBucket)correlation;
        List left=Arrays.asList(correl_bucket.getLeftKeys());
        List right=Arrays.asList(correl_bucket.getRightKeys());
        List correlList=Arrays.asList(correl_bucket.getCorrelationValues());

        assertTrue(left.contains(firstPath)&&right.contains(secondPath)||left.contains(secondPath)&&right.contains(firstPath));

        Histogram histo = response.getAggregations().get("time_buckets");
        assertThat(histo.getName(), equalTo("time_buckets"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        List<Double> sums = Lists.newArrayList();
        List<Double> partialSums = Lists.newArrayList();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket_result = buckets.get(i);
            assertThat(bucket_result, notNullValue());

            InternalSum overallSumValue = bucket_result.getAggregations().get("overall");
            assertThat(overallSumValue, notNullValue());
            double sumValue = overallSumValue.value();
            sums.add(sumValue);
            InternalSum partialSumValue = ((InternalFilter)bucket_result.getAggregations().get("filter")).getAggregations().get("sum_partial");
            assertThat(partialSumValue, notNullValue());

            double partialSum = partialSumValue.value();
            partialSums.add(partialSum);
        }

        double correlValue = correlation(sums.toArray(new Double[sums.size()]), partialSums.toArray(new Double[partialSums.size()]));
        logger.info("correlList"+((double[])correlList.get(0))[0]);
        logger.info("correlValue"+correlValue);
        assertTrue(Math.abs(((double[])correlList.get(0))[0])-correlValue<1e-5);
    }

    public static double correlation(Double[] xs, Double[] ys) {
        //TODO: check here that arrays are not null, of the same length etc

        double sx = 0.0;
        double sy = 0.0;
        double sxx = 0.0;
        double syy = 0.0;
        double sxy = 0.0;

        int n = xs.length;

        for(int i = 0; i < n; ++i) {
            double x = xs[i];
            double y = ys[i];

            sx += x;
            sy += y;
            sxx += x * x;
            syy += y * y;
            sxy += x * y;
        }

        // covariation
        double cov = sxy / n - sx * sy / n / n;
        // standard error of x
        double sigmax = Math.sqrt(sxx / n -  sx * sx / n / n);
        // standard error of y
        double sigmay = Math.sqrt(syy / n -  sy * sy / n / n);

        // correlation is just a normalized covariation
        return cov / sigmax / sigmay;
    }

}
