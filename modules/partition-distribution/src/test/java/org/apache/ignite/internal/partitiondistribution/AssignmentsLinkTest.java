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

package org.apache.ignite.internal.partitiondistribution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.junit.jupiter.api.Test;

class AssignmentsLinkTest {
    private static final long BASE_PHYSICAL_TIME = LocalDateTime.of(2024, Month.JANUARY, 1, 0, 0)
            .atOffset(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli();
    
    private static final Assignments ASSIGNMENTS0_4 = Assignments.of(
            Set.of(
                    Assignment.forPeer("node0"),
                    Assignment.forPeer("node1"),
                    Assignment.forPeer("node2"),
                    Assignment.forPeer("node3"),
                    Assignment.forPeer("node4")
            ),
            baseTimestamp(1)
    );

    private static final Assignments ASSIGNMENTS0_2 = Assignments.of(
            Set.of(
                    Assignment.forPeer("node0"),
                    Assignment.forPeer("node1"),
                    Assignment.forPeer("node2")
            ),
            baseTimestamp(2)
    );

    private static final Assignments ASSIGNMENTS_2 = Assignments.of(
            Set.of(
                    Assignment.forPeer("node2")
            ),
            baseTimestamp(3)
    );

    private static long baseTimestamp(int logical) {
        return new HybridTimestamp(BASE_PHYSICAL_TIME, logical).longValue();
    }

    @Test
    void testLastLink() {
        AssignmentsChain chain = AssignmentsChain.of(ASSIGNMENTS0_4);
        AssignmentsLink link1 = chain.firstLink();

        AssignmentsLink link2 = chain.addLast(ASSIGNMENTS0_2, 1, 1);

        AssignmentsLink link3 = chain.addLast(ASSIGNMENTS_2, 2, 2);

        assertThat(chain.lastLink("node0"), is(link2));
        assertThat(chain.lastLink("node1"), is(link2));
        assertThat(chain.lastLink("node2"), is(link3));
        assertThat(chain.lastLink("node3"), is(link1));
        assertThat(chain.lastLink("node4"), is(link1));
        assertThat(chain.lastLink("node10"), is(nullValue()));
    }

    @Test
    void testNextLink() {
        AssignmentsChain chain = AssignmentsChain.of(ASSIGNMENTS0_4);

        AssignmentsLink link2 = chain.addLast(ASSIGNMENTS0_2, 1, 1);

        AssignmentsLink link3 = chain.addLast(ASSIGNMENTS_2, 2, 2);

        AssignmentsLink link1 = chain.firstLink();

        assertThat(link1.assignments(), is(ASSIGNMENTS0_4));

        assertThat(link1.nextLink(), is(link2));
        assertThat(link1.nextLink().nextLink(), is(link3));
        assertThat(link1.nextLink().nextLink().nextLink(), is(nullValue()));

        assertThat(link2.nextLink(), is(link3));
        assertThat(link2.nextLink().nextLink(), is(nullValue()));

        assertThat(link3.nextLink(), is(nullValue()));
    }
}
