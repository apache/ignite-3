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

package org.apache.ignite.internal.network;

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class OutNetworkObjectTest extends BaseIgniteAbstractTest {
    private final OutNetworkObject outObject = new OutNetworkObject(mock(NetworkMessage.class), emptyList(), true);

    @Test
    void acknowledgementFutureIsIncompleteInitially() {
        assertThat(outObject.acknowledgedFuture(), is(not(completedFuture())));
    }

    @Test
    void acknowledgeCompletesFuture() {
        outObject.acknowledge();

        assertThat(outObject.acknowledgedFuture(), is(completedFuture()));
    }

    @Test
    void failsAcknowledgementFuture() {
        Exception ex = new Exception();

        outObject.failAcknowledgement(ex);

        Exception resultEx = assertWillThrowFast(outObject.acknowledgedFuture(), Exception.class);
        assertThat(resultEx, is(ex));
    }
}
