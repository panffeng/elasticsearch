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



import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.pipeline.*;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.*;

import java.util.stream.Collectors;


import static java.util.stream.Collectors.groupingBy;

/**
 * computing model of the logics
 */
public class CorrelationPipelineAggregator extends SiblingPipelineAggregator {
    protected final DocValueFormat formatter;
    protected final GapPolicy gapPolicy;
    protected final int lag;
    protected final Map<String, String> bucketsPathsMap;


    protected Map<String,Map<String,Double>> timeSeries = new HashMap<>();

    private final Logger logger = Loggers.getLogger(getClass());


    public CorrelationPipelineAggregator(String name, Map<String, String> bucketsPathsMap, int lag, DocValueFormat formatter,
                                         GapPolicy gapPolicy, Map<String, Object> metadata) {
        super(name, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]),  metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.lag = lag;
        this.formatter=formatter;
        this.gapPolicy = gapPolicy;
    }
    /**
     * Read from a stream.
     */
    @SuppressWarnings("unchecked")
    public CorrelationPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        lag = in.readVInt();
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
        bucketsPathsMap = (Map<String, String>) in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(lag);
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
        out.writeGenericValue(bucketsPathsMap);
    }

    @Override
    public String getWriteableName() {
        return CorrelationPipelineAggregationBuilder.NAME;
    }


    @Override
    public final InternalAggregation doReduce(Aggregations aggregations, ReduceContext context) {
        preCollection();
        for(String onebucketsPath:bucketsPaths()) {
            List<String> bucketsPath = AggregationPath.parse(onebucketsPath).getPathElementsAsStringList();
            for (Aggregation aggregation : aggregations) {
                if (aggregation.getName().equals(bucketsPath.get(0))) {
                    bucketsPath = bucketsPath.subList(1, bucketsPath.size());
                    InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                    List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                    for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                        Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, bucketsPath, gapPolicy);
                        if (bucketValue != null && !Double.isNaN(bucketValue)) {
                            collectBucketValue(bucket.getKeyAsString(), bucketValue, onebucketsPath);
                        }
                    }
                }
            }
        }
        return buildAggregation(Collections.emptyList(), metaData());
    }


    protected void preCollection() {
        timeSeries.clear();
    }


    protected InternalAggregation buildAggregation(List<PipelineAggregator> pipelineAggregators, Map<String, Object> metadata) {
        Collection<Map<String,Double>> values = timeSeries.values();

        Optional<Integer> tupleSizeOptional = values.stream().map((x) -> x.size()).max(Comparator.naturalOrder());

        String[] leftKeysArray=null;
        String[] rightKeysArray=null;
        double[] correlationValuesArray=null;
        if(tupleSizeOptional!=null){
            Integer tupleSize = tupleSizeOptional.get();
            List<Map<String,Double>> filteredTimeSeries = values.stream().filter((x) -> x.size() == tupleSize).collect(Collectors.toList());

            Map<String,List<Double>> listsOfPaths = filteredTimeSeries.stream()
                    .flatMap(m -> m.entrySet().stream())
                    .collect(groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
            Set<String> keys = listsOfPaths.keySet();
//            List<AbstractMap.SimpleEntry> namedLists = keys.stream().flatMap(left -> keys.stream().filter(right-> right.compareTo(left)>0).map(right ->
//                    new AbstractMap.SimpleEntry<String[],Double>(new String[]{left, right}, correlation(listsOfPaths.get(left), listsOfPaths.get(right))))).collect(Collectors.toList());
            List<String> leftKeys=new ArrayList<>();
            List<String> rightKeys=new ArrayList<>();
            List<Double> correls = new ArrayList<>();
            for(String left:keys){
                for(String right:keys){
                    if(left.compareTo(right)>0){
                        leftKeys.add(left);
                        rightKeys.add(right);
                        correls.add(correlation(listsOfPaths.get(left), listsOfPaths.get(right)));
                    }
                }
            }
            leftKeysArray=leftKeys.toArray(new String[leftKeys.size()]);
            rightKeysArray=rightKeys.toArray(new String[rightKeys.size()]);
            correlationValuesArray=new double[correls.size()];
            {int i=0;
                for(Double val:correls)correlationValuesArray[i++]=val;}
        }





        logger.info("the timeSeries for correlation"+timeSeries);


        return new InternalCorrelationBucket(name(), leftKeysArray,rightKeysArray,correlationValuesArray, formatter, pipelineAggregators, metadata);
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



    private Double correlation(List<Double> left, List<Double> right) {
        if(left!=null&&right!=null&&left.size()==right.size()){
            Double[] xs=left.toArray(new Double[left.size()]);
            Double[] ys=right.toArray(new Double[right.size()]);
            return correlation(xs,ys);
        }

        return Double.NaN;

    }



    protected void collectBucketValue(String bucketKey, Double bucketValue, String bucketPath) {
        Map<String,Double> bucketValues = timeSeries.get(bucketKey);
        if(bucketValues==null){
            bucketValues = new HashMap<>();
        }
        if(bucketValue!=null){
            bucketValues.put(bucketPath,bucketValue);
        }
        timeSeries.put(bucketKey,bucketValues);
    }


}
