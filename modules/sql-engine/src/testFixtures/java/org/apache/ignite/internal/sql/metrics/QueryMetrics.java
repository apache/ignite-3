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

package org.apache.ignite.internal.sql.metrics;

import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.CANCELED_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.FAILED_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.SUCCESSFUL_QUERIES;
import static org.apache.ignite.internal.sql.metrics.SqlQueryMetricSource.TIMED_OUT_QUERIES;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.function.ToLongFunction;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/** Snapshot of query execution metric counters captured at a point in time. */
public class QueryMetrics {
    private final long succeeded;
    private final long failed;
    private final long canceled;
    private final long timedOut;

    /**
     * Creates a snapshot by reading metric values using the given function.
     *
     * @param metricValue Function that returns the current value for a given metric name.
     */
    public QueryMetrics(ToLongFunction<String> metricValue) {
        succeeded = metricValue.applyAsLong(SUCCESSFUL_QUERIES);
        failed = metricValue.applyAsLong(FAILED_QUERIES);
        canceled = metricValue.applyAsLong(CANCELED_QUERIES);
        timedOut = metricValue.applyAsLong(TIMED_OUT_QUERIES);
    }

    /**
     * Polls the given metric value function until the metric deltas (relative to this snapshot) match the expected values.
     *
     * @param metricValue Function that returns the current value for a given metric name.
     * @param succeededDelta Expected increase in succeeded count.
     * @param failedDelta Expected increase in failed count.
     * @param canceledDelta Expected increase in canceled count.
     * @param timedOutDelta Expected increase in timed-out count.
     */
    public void awaitDeltas(
            ToLongFunction<String> metricValue,
            long succeededDelta,
            long failedDelta,
            long canceledDelta,
            long timedOutDelta
    ) {
        await().pollDelay(Duration.ZERO).until(
                () -> new QueryMetrics(metricValue),
                hasDeltas(succeededDelta, failedDelta, canceledDelta, timedOutDelta)
        );
    }

    /**
     * Returns a Hamcrest matcher that checks whether the actual {@link QueryMetrics} equals this snapshot plus the given deltas. Intended
     * for use with Awaitility to poll until asynchronously updated metrics reach the expected values.
     *
     * @param succeededDelta Expected increase in succeeded count.
     * @param failedDelta Expected increase in failed count.
     * @param canceledDelta Expected increase in canceled count.
     * @param timedOutDelta Expected increase in timed-out count.
     */
    private Matcher<QueryMetrics> hasDeltas(long succeededDelta, long failedDelta, long canceledDelta, long timedOutDelta) {
        long expectedSucceeded = succeeded + succeededDelta;
        long expectedFailed = failed + failedDelta;
        long expectedCanceled = canceled + canceledDelta;
        long expectedTimedOut = timedOut + timedOutDelta;

        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(QueryMetrics actual) {
                return actual.succeeded == expectedSucceeded
                        && actual.failed == expectedFailed
                        && actual.canceled == expectedCanceled
                        && actual.timedOut == expectedTimedOut;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("metrics [succeeded=").appendValue(expectedSucceeded)
                        .appendText(", failed=").appendValue(expectedFailed)
                        .appendText(", canceled=").appendValue(expectedCanceled)
                        .appendText(", timedOut=").appendValue(expectedTimedOut)
                        .appendText("]");
            }

            @Override
            protected void describeMismatchSafely(QueryMetrics actual, Description mismatchDescription) {
                boolean first = true;
                if (actual.succeeded != expectedSucceeded) {
                    mismatchDescription.appendText("succeeded was ").appendValue(actual.succeeded);
                    first = false;
                }
                if (actual.failed != expectedFailed) {
                    if (!first) {
                        mismatchDescription.appendText(", ");
                    }
                    mismatchDescription.appendText("failed was ").appendValue(actual.failed);
                    first = false;
                }
                if (actual.canceled != expectedCanceled) {
                    if (!first) {
                        mismatchDescription.appendText(", ");
                    }
                    mismatchDescription.appendText("canceled was ").appendValue(actual.canceled);
                    first = false;
                }
                if (actual.timedOut != expectedTimedOut) {
                    if (!first) {
                        mismatchDescription.appendText(", ");
                    }
                    mismatchDescription.appendText("timedOut was ").appendValue(actual.timedOut);
                }
            }
        };
    }
}
