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
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteInternalException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link NettyServer}.
 */
public class NettyServerTest {
    /**
     * Tests a successful server start scenario.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessfulServerStart() throws Exception {
        var channel = new EmbeddedServerChannel();

        NettyServer server = getServer(channel, true);

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

        NettyServer server = getServer(channel, true);

        server.stop().join();

        assertTrue(server.getBossGroup().isTerminated());
        assertTrue(server.getWorkerGroup().isTerminated());
    }

    /**
     * Tests an unsuccessful server start scenario.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerFailedToStart() throws Exception {
        var channel = new EmbeddedServerChannel();

        NettyServer server = getServer(channel, false);

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

        NettyServer server = getServer(channel, true);

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

        NettyServer server = getServer(channel, true);

        assertThrows(IgniteInternalException.class, server::start);
    }

    /**
     * Creates a server from a backing {@link ChannelFuture}.
     *
     * @param channel Server channel.
     * @param shouldStart {@code true} if a server should start successfully
     * @return NettyServer.
     * @throws Exception If failed.
     */
    private static NettyServer getServer(EmbeddedServerChannel channel, boolean shouldStart) throws Exception {
        ServerBootstrap bootstrap = Mockito.spy(new ServerBootstrap());

        Mockito.doReturn(shouldStart ? channel.newSucceededFuture() : channel.newFailedFuture(new ClosedChannelException()))
            .when(bootstrap).bind(Mockito.anyInt());

        var server = new NettyServer(bootstrap, 0, null, null, null);

        try {
            server.start().get(3, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            if (shouldStart)
                fail(e);
        }

        return server;
    }

    /** Server channel on top of the {@link EmbeddedChannel}. */
    private static class EmbeddedServerChannel extends EmbeddedChannel implements ServerChannel {
        // No-op.
    }
}
