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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.UUID;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.utils.Clients;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.junit.jupiter.api.AfterAll;

class ItThinClientComputeEventsTest extends ItComputeEventsTest {
    private final Clients clients = new Clients();

    @AfterAll
    void cleanup() {
        clients.cleanup();
    }

    @Override
    protected IgniteCompute compute() {
        return clients.compute(node(0));
    }

    @Override
    protected EventMatcher jobEvent(IgniteEventType eventType, Type jobType, UUID jobId, String jobClassName, String targetNode) {
        return super.jobEvent(eventType, jobType, jobId, jobClassName, targetNode)
                .withUsername(is("unknown"))
                .withClientAddress(notNullValue(String.class));
    }
}
