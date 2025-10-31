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

package org.apache.ignite.internal.raft;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.raft.jraft.core.NodeImpl.LEADER_STEPPED_DOWN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.util.DirectExecutor;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.junit.jupiter.api.Test;

class JraftGroupEventsListenerTest extends BaseIgniteAbstractTest {
    /**
     * Tests that {@link JraftGroupEventsListener} handles correctly a situation when
     * ConfigurationCtx#reset is called with null status.
     *
     * @throws Exception If failed.
     */
    @Test
    void testOnReconfigurationErrorCalledFromResetWithNullStatus() throws Exception {
        JraftGroupEventsListener listener = mock(JraftGroupEventsListener.class);

        NodeImpl node = mock(NodeImpl.class);

        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setCommonExecutor(new DirectExecutor());
        nodeOptions.setRaftGrpEvtsLsnr(listener);

        when(node.getOptions()).thenReturn(nodeOptions);

        Class<?> confCtxClass = Class.forName("org.apache.ignite.raft.jraft.core.NodeImpl$ConfigurationCtx");

        Constructor<?> constructor = confCtxClass.getDeclaredConstructor(NodeImpl.class);
        constructor.setAccessible(true);

        // ConfigurationCtx object.
        Object confCtx = constructor.newInstance(node);

        var resultFuture = new CompletableFuture<Status>();

        IgniteTestUtils.setFieldValue(confCtx, "done", (Closure) resultFuture::complete);

        Method resetMethod = confCtxClass.getDeclaredMethod("reset", Status.class);
        resetMethod.setAccessible(true);

        // Execute reset method with null status
        resetMethod.invoke(confCtx, new Object[]{null});

        Status defaultStatus = LEADER_STEPPED_DOWN;

        // onReconfigurationError should not be called with null status but rather with a default status.
        verify(listener, times(1)).onReconfigurationError(eq(defaultStatus), any(), any(), anyLong(), anyLong());

        // Future should be already done as execution is in the same thread.
        assertTrue(resultFuture.isDone());
        assertThat(resultFuture, willBe(defaultStatus));
    }
}
