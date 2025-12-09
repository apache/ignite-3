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

import static io.scalecube.cluster.membership.MembershipEvent.createAdded;
import static io.scalecube.cluster.membership.MembershipEvent.createRemoved;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.scalecube.cluster.Member;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.net.Address;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class ScaleCubeTopologyServiceTest {
    private final ScaleCubeTopologyService topologyService = new ScaleCubeTopologyService();

    private final Member member1 = new Member(new UUID(0, 1).toString(), "first", Address.create("host", 1001), "default");
    private final Member member2 = new Member(new UUID(0, 2).toString(), "second", Address.create("host", 1002), "default");

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

        topologyService.onMembershipEvent(createRemoved(member2, null, 100));

        assertThat(topologyService.allMembers(), hasSize(1));
        assertThat(topologyService.getByConsistentId("second"), is(nullValue()));
    }

    private void addTwoMembers() {
        topologyService.onMembershipEvent(createAdded(member1, null, 1));
        topologyService.onMembershipEvent(createAdded(member2, null, 2));
    }

    @Test
    void leavingEventRemovesNodeFromTopology() {
        addTwoMembers();

        topologyService.onMembershipEvent(MembershipEvent.createLeaving(member2, null, 100));

        assertThat(topologyService.allMembers(), hasSize(1));
        assertThat(topologyService.getByConsistentId("second"), is(nullValue()));
    }

    @Test
    void getByConsistentIdWorksWithConcurrentModifications(@InjectExecutorService ExecutorService executor) {
        testGetNodeWorksWithConcurrentModifications(executor, member -> topologyService.getByConsistentId(member.alias()));
    }

    @Test
    void getByAddressWorksWithConcurrentModifications(@InjectExecutorService ExecutorService executor) {
        testGetNodeWorksWithConcurrentModifications(
                executor,
                member -> topologyService.getByAddress(new NetworkAddress(member.address().host(), member.address().port()))
        );
    }

    @Test
    void getByIdWorksWithConcurrentModifications(@InjectExecutorService ExecutorService executor) {
        testGetNodeWorksWithConcurrentModifications(executor, member -> topologyService.getById(UUID.fromString(member.id())));
    }

    private void testGetNodeWorksWithConcurrentModifications(
            @InjectExecutorService ExecutorService executor,
            Function<Member, InternalClusterNode> getter
    ) {
        Member member = new Member(randomUUID().toString(), "test", Address.create("host", 1024), "default");

        AtomicBoolean proceed = new AtomicBoolean(true);

        CompletableFuture<Void> updaterFuture = runAsync(() -> {
            for (int i = 0; i < 1000 && proceed.get(); i++) {
                topologyService.onMembershipEvent(createAdded(member, null, System.nanoTime()));
                topologyService.onMembershipEvent(createRemoved(member, null, System.nanoTime()));
            }
        }, executor).whenComplete((res, ex) -> proceed.set(false));

        CompletableFuture<Void> readerFuture = runAsync(() -> {
            while (proceed.get()) {
                assertDoesNotThrow(() -> getter.apply(member));
            }
        }, executor).whenComplete((res, ex) -> proceed.set(false));

        assertThat(allOf(updaterFuture, readerFuture), willCompleteSuccessfully());
    }

    @Test
    void readdedEventReplacesNodeInTopology() {
        Member member1NewVersion = new Member(randomUUID().toString(), member1.alias(), member1.address(), member1.namespace());

        topologyService.onMembershipEvent(createAdded(member1, null, 1));
        topologyService.onMembershipEvent(createAdded(member1NewVersion, null, 2));

        assertThat(topologyService.allMembers(), hasSize(1));

        assertThat(topologyService.getById(UUID.fromString(member1.id())), is(nullValue()));

        InternalClusterNode firstById = topologyService.getById(UUID.fromString(member1NewVersion.id()));
        assertThatMatchesFirstMemberNewVersion(firstById, member1NewVersion);

        InternalClusterNode firstByConsistentId = topologyService.getByConsistentId("first");
        assertThatMatchesFirstMemberNewVersion(firstByConsistentId, member1NewVersion);

        InternalClusterNode firstByAddress = topologyService.getByAddress(
                new NetworkAddress(member1.address().host(), member1.address().port())
        );
        assertThatMatchesFirstMemberNewVersion(firstByAddress, member1NewVersion);
    }

    private static void assertThatMatchesFirstMemberNewVersion(@Nullable InternalClusterNode clusterNode, Member member1NewVersion) {
        assertThat(clusterNode, is(notNullValue()));
        assertThat(clusterNode.name(), is("first"));
        assertThat(clusterNode.id().toString(), is(member1NewVersion.id()));
    }
}
