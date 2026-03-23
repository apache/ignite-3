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

package org.apache.ignite.internal.compute.events;

import static org.apache.ignite.internal.compute.events.EventMatcher.computeJobEvent;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_QUEUED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.notNullValue;

import java.util.UUID;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.Test;

class EventMatcherTest {
    @Test
    void mismatch() {
        UUID jobId = UUID.randomUUID();
        String event = createEvent(jobId);
        EventMatcher matcher = computeJobEvent(COMPUTE_JOB_QUEUED)
                .withTimestamp(is(notNullValue(Long.class)))
                .withProductVersion(is(matchesRegex(IgniteProductVersion.VERSION_PATTERN)))
                .withUsername(is(any(String.class)))
                .withType("SINGLE")
                .withClassName("foo")
                .withJobId(jobId)
                .withTargetNode("bar");

        assertThat(matcher.matches(event), is(false));
        StringDescription description = new StringDescription();
        matcher.describeMismatch(event, description);
        assertThat(description.toString(), is("class name was \"JobClass\" and target node was \"node\""));
    }

    @Test
    void match() {
        UUID jobId = UUID.randomUUID();
        String event = createEvent(jobId);

        assertThat(event, is(computeJobEvent(COMPUTE_JOB_QUEUED)
                .withTimestamp(is(notNullValue(Long.class)))
                .withProductVersion(is(matchesRegex(IgniteProductVersion.VERSION_PATTERN)))
                .withUsername(is(any(String.class)))
                .withType("SINGLE")
                .withClassName("JobClass")
                .withJobId(jobId)
                .withTargetNode("node")
        ));
    }

    @Test
    void description() {
        UUID jobId = UUID.randomUUID();

        StringDescription description = new StringDescription();

        EventMatcher matcher = computeJobEvent(COMPUTE_JOB_QUEUED)
                .withTimestamp(anything())
                .withProductVersion(anything())
                .withUsername(anything())
                .withType("SINGLE")
                .withClassName("foo")
                .withJobId(jobId)
                .withTargetNode("bar");

        matcher.describeTo(description);

        assertThat(description.toString(), is("event of type is \"COMPUTE_JOB_QUEUED\" and timestamp that ANYTHING "
                + "and product version that ANYTHING and username that ANYTHING and type that is \"SINGLE\" "
                + "and class name that is \"foo\" and job ID that is <" + jobId + "> "
                + "and target node that is \"bar\""));
    }

    private static String createEvent(UUID jobId) {
        return "{"
                + "\"type\":\"COMPUTE_JOB_QUEUED\","
                + "\"timestamp\":1754040984817,"
                + "\"productVersion\":\"3.1.0-SNAPSHOT\","
                + "\"user\":{"
                + "  \"username\":\"SYSTEM\","
                + "  \"authenticationProvider\":\"SYSTEM\""
                + "},"
                + "\"fields\":{"
                + "  \"type\":\"SINGLE\","
                + "  \"className\":\"JobClass\","
                + "  \"jobId\":\"" + jobId + "\","
                + "  \"targetNode\":\"node\""
                + "}}";
    }
}
