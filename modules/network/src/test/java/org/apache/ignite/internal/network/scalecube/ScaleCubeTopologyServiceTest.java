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

package org.apache.ignite.internal.network.scalecube;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.net.Address;
import org.junit.jupiter.api.Test;

class ScaleCubeTopologyServiceTest {
    private final ScaleCubeTopologyService topologyService = new ScaleCubeTopologyService();

    private final Member member1 = new Member("id1", "first", Address.create("host", 1001), "default");
    private final Member member2 = new Member("id2", "second", Address.create("host", 1002), "default");

    @Test
    void addedEventAddsNodeToTopology() {
        addTwoMembers();

        assertThat(topologyService.allMembers(), hasSize(2));
        assertThat(topologyService.getByConsistentId("first").name(), is("first"));
        assertThat(topologyService.getByConsistentId("second").name(), is("second"));
    }

    @Test
    void removedEventRemovesNodeFromTopology() {
        addTwoMembers();

        topologyService.onMembershipEvent(MembershipEvent.createRemoved(member2, null, 100));

        assertThat(topologyService.allMembers(), hasSize(1));
        assertThat(topologyService.getByConsistentId("second"), is(nullValue()));
    }

    private void addTwoMembers() {
        topologyService.onMembershipEvent(MembershipEvent.createAdded(member1, null, 1));
        topologyService.onMembershipEvent(MembershipEvent.createAdded(member2, null, 2));
    }

    @Test
    void leavingEventRemovesNodeFromTopology() {
        addTwoMembers();

        topologyService.onMembershipEvent(MembershipEvent.createLeaving(member2, null, 100));

        assertThat(topologyService.allMembers(), hasSize(1));
        assertThat(topologyService.getByConsistentId("second"), is(nullValue()));
    }
}
