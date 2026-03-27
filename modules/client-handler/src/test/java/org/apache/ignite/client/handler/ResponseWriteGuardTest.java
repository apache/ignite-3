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

package org.apache.ignite.client.handler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundInvoker;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ResponseWriteGuard}.
 */
class ResponseWriteGuardTest {
    @Test
    void testWriteHappensOnce() {
        var guard = new ResponseWriteGuard();
        var ctx = mock(ChannelOutboundInvoker.class);
        var buf1 = mock(ByteBuf.class);
        var buf2 = mock(ByteBuf.class);

        boolean first = guard.write(ctx, buf1);
        boolean second = guard.write(ctx, buf2);

        assertTrue(first);
        assertFalse(second);

        // Second write should not reach ctx
        verify(ctx, times(1)).write(buf1);
        verify(ctx, times(0)).write(buf2);
    }
}
