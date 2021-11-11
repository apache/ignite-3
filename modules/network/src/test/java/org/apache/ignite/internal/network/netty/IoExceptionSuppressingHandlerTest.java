/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.netty;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Test class for the {@link IoExceptionSuppressingHandler}.
 */
public class IoExceptionSuppressingHandlerTest {
    private final ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    
    private final IoExceptionSuppressingHandler handler = new IoExceptionSuppressingHandler();

    /**
     * Tests that a "Broken pipe" exception is muted by the handler.
     */
    @Test
    public void testBrokenPipeIoExceptionIsMuted() {
        handler.exceptionCaught(context, new IOException("Broken pipe"));

        verify(context, never()).fireExceptionCaught(any());
    }

    /**
     * Tests that other exception types are porpagated.
     */
    @Test
    public void testOtherExceptionIsPropagated() {
        handler.exceptionCaught(context, new NullPointerException());

        verify(context).fireExceptionCaught(any());
    }
}
