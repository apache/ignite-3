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

package org.apache.ignite.internal.network.recovery;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import io.netty.channel.Channel;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DescriptorAcquiryTest extends BaseIgniteAbstractTest {
    @Mock
    private Channel channel;

    private final CompletableFuture<NettySender> handshakeCompleteFuture = new CompletableFuture<>();

    @Test
    void clinchResolvedStagedIsInitiallyIncomplete() {
        DescriptorAcquiry acquiry = new DescriptorAcquiry(channel, handshakeCompleteFuture);

        assertThat(acquiry.clinchResolved().toCompletableFuture(), is(not(completedFuture())));
    }

    @Test
    void clinchGetsResolved() {
        DescriptorAcquiry acquiry = new DescriptorAcquiry(channel, handshakeCompleteFuture);

        acquiry.markClinchResolved();

        assertThat(acquiry.clinchResolved().toCompletableFuture(), is(completedFuture()));
    }
}
