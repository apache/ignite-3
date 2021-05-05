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

package org.apache.ignite.network.internal.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ServerChannel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link NettyServer}.
 */
public class NettyServerTest {
    /**
     * Tests a successfull server start scenario.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessfullServerStart() throws Exception {
        var channel = new EmbeddedServerChannel();

        NettyServer server = getServer(channel.newSucceededFuture());

        assertTrue(server.isRunning());
    }

    /**
     * Tests a graceful server shutdown scenario.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerGracefulShutdown() throws Exception {
        var channel = new EmbeddedServerChannel();

        NettyServer server = getServer(channel.newSucceededFuture());

        server.stop().join();

        assertTrue(server.getBossGroup().isTerminated());
        assertTrue(server.getWorkerGroup().isTerminated());
    }

    /**
     * Tests a non-graceful server shutdown scenario.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerChannelClosedAbruptly() throws Exception {
        var channel = new EmbeddedServerChannel();

        NettyServer server = getServer(channel.newSucceededFuture());

        channel.close();

        assertTrue(server.getBossGroup().isShuttingDown());
        assertTrue(server.getWorkerGroup().isShuttingDown());
    }

    /**
     * Tests that a {@link NettyServer#start} method can be called only once.
     *
     * @throws Exception
     */
    @Test
    public void testStartTwice() throws Exception {
        var channel = new EmbeddedServerChannel();

        NettyServer server = getServer(channel.newSucceededFuture());

        assertThrows(IgniteInternalException.class, () -> {
            server.start();
        });
    }

    /**
     * Creates a server from a backing {@link ChannelFuture}.
     *
     * @param future Channel future.
     * @return NettyServer.
     * @throws Exception If failed.
     */
    private NettyServer getServer(ChannelFuture future) throws Exception {
        ServerBootstrap bootstrap = Mockito.spy(new ServerBootstrap());

        Mockito.doReturn(future).when(bootstrap).bind(Mockito.anyInt());

        var server = new NettyServer(bootstrap, 0, null, null, null);

        server.start().get(3, TimeUnit.SECONDS);

        return server;
    }

    /** Server channel on top of the {@link EmbeddedChannel}. */
    private static class EmbeddedServerChannel extends EmbeddedChannel implements ServerChannel {
        // No-op.
    }
}
