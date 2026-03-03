/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metrics;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Arrays;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

/**
 * Hamcrest matchers for testing metrics.
 */
public final class MetricMatchers {
    private MetricMatchers() {
        // No-op.
    }

    /**
     * Creates a matcher that matches a {@link DistributionMetric} with the expected total number of measurements
     * across all histogram buckets.
     *
     * @param expectedMeasuresCount Expected total number of measurements across all buckets.
     * @return Matcher for distribution metric measures count.
     */
    public static Matcher<DistributionMetric> hasMeasurementsCount(long expectedMeasuresCount) {
        return new FeatureMatcher<>(
                is(expectedMeasuresCount),
                "a DistributionMetric with measures count",
                "measures count") {
            @Override
            protected Long featureValueOf(DistributionMetric metric) {
                return Arrays.stream(metric.value()).sum();
            }
        };
    }

    /**
     * Creates a matcher that matches a {@link LongMetric} whose value satisfies the given matcher.
     *
     * @param valueMatcher Matcher for the metric value.
     * @return Matcher for long metric value.
     */
    public static Matcher<LongMetric> hasValue(Matcher<Long> valueMatcher) {
        return new FeatureMatcher<>(
                valueMatcher,
                "a LongMetric with value",
                "value") {
            @Override
            protected Long featureValueOf(LongMetric metric) {
                return metric.value();
            }
        };
    }

    /**
     * Creates a matcher that matches a {@link MetricSet} containing a metric with the given name
     * that satisfies the given matcher.
     *
     * @param name Name of the metric to look up in the metric set.
     * @param metricMatcher Matcher for the metric.
     * @param <M> Type of the metric.
     * @return Matcher for metric set containing the specified metric.
     */
    public static <M extends Metric> Matcher<MetricSet> hasMetric(String name, Matcher<M> metricMatcher) {
        return new FeatureMatcher<>(
                allOf(notNullValue(), metricMatcher),
                "a MetricSet with metric named \"" + name + "\"",
                "metric named \"" + name + "\"") {
            @Override
            protected M featureValueOf(MetricSet metricSet) {
                return metricSet.get(name);
            }
        };
    }
}
