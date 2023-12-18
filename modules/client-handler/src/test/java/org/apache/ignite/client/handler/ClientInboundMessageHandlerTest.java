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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ignite.client.handler.configuration.ClientConnectorConfiguration;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestValidationUtil;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.security.authentication.AuthenticationManager;
import org.apache.ignite.internal.security.authentication.AuthenticationManagerImpl;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.configuration.validator.AuthenticationProvidersValidator;
import org.apache.ignite.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.security.configuration.SecurityChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.msgpack.core.MessagePack;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
class ClientInboundMessageHandlerTest extends BaseIgniteAbstractTest {
    private static final Duration TIMEOUT_OF_DURING = Duration.ofSeconds(2);

    private static final String PROVIDER_NAME = "basic";

    @InjectConfiguration
    private ClientConnectorConfiguration configuration;

    @InjectConfiguration
    private SecurityConfiguration securityConfiguration;

    @Mock
    private IgniteTablesInternal igniteTables;

    @Mock
    private IgniteTransactionsImpl igniteTransactions;

    @Mock
    private QueryProcessor processor;

    @Mock
    private IgniteCompute compute;

    @Mock
    private TopologyService topologyService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private IgniteSql sql;

    @Mock
    private CompletableFuture<UUID> clusterId;

    @Mock
    private ClientHandlerMetricSource metrics;

    @Mock
    private HybridClock clock;

    @Mock
    private SchemaSyncService schemaSyncService;

    @Mock
    private CatalogService catalogService;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @Mock
    private ChannelFuture channelFuture;

    private ClientInboundMessageHandler handler;

    private final AtomicBoolean ctxClosed = new AtomicBoolean(false);

    @BeforeEach
    void setUp() throws Exception {
        doReturn(topologyService).when(clusterService).topologyService();

        ClusterNode node = new ClusterNodeImpl("node1", "node1", new NetworkAddress("localhost", 10800));
        doReturn(node).when(topologyService).localMember();

        doReturn(UUID.randomUUID()).when(clusterId).join();

        doReturn(channelFuture).when(channel).closeFuture();

        doReturn(new UnpooledByteBufAllocator(true)).when(ctx).alloc();
        doReturn(channel).when(ctx).channel();
        lenient().doAnswer(invocation -> {
            ctxClosed.set(true);
            return null;
        }).when(ctx).close();

        AuthenticationManager authenticationManager = new AuthenticationManagerImpl();
        AtomicLong clientIdGen = new AtomicLong(0);

        handler = new ClientInboundMessageHandler(
                igniteTables,
                igniteTransactions,
                processor,
                configuration.value(),
                compute,
                clusterService,
                sql,
                clusterId,
                metrics,
                authenticationManager,
                clock,
                schemaSyncService,
                catalogService,
                clientIdGen.incrementAndGet(),
                new ClientPrimaryReplicaTracker(null, catalogService, clock, schemaSyncService)
        );

        authenticationManager.listen(handler);
        securityConfiguration.listen(authenticationManager);

        changeConfiguration(change -> {
            change.changeEnabled(true);
            change.changeAuthentication().changeProviders()
                    .create(PROVIDER_NAME, providerChange -> {
                        providerChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers()
                                .create("admin", user -> user.changePassword("password"))
                                .create("admin1", user -> user.changePassword("password"));
                    });
        });

        handler.channelRegistered(ctx);
    }

    @Test
    void disableAuthentication() throws IOException {
        handshake();

        changeConfiguration(change -> change.changeEnabled(false));

        await().during(TIMEOUT_OF_DURING).untilAtomic(ctxClosed, is(false));
    }

    @Test
    void enableAuthentication() throws IOException {
        changeConfiguration(change -> change.changeEnabled(false));

        handshake();

        changeConfiguration(change -> change.changeEnabled(true));

        await().untilAtomic(ctxClosed, is(true));
    }

    @Test
    void changeProvider() throws IOException {
        handshake();

        changeConfiguration(change -> {
            change.changeEnabled(true);
            change.changeAuthentication().changeProviders().update(PROVIDER_NAME, providerChange -> {
                providerChange.convert(BasicAuthenticationProviderChange.class)
                        .changeUsers().update("admin", user -> user.changePassword("new-password"));
            });
        });

        await().untilAtomic(ctxClosed, is(true));
    }

    private void handshake() throws IOException {
        var packer = MessagePack.newDefaultBufferPacker();
        packer.packInt(3); // Major.
        packer.packInt(0); // Minor.
        packer.packInt(0); // Patch.

        packer.packInt(2); // Client type: general purpose.

        packer.packBinaryHeader(0); // Features.
        packer.packInt(3); // Extensions.
        packer.packString("authn-type");
        packer.packString(PROVIDER_NAME);
        packer.packString("authn-identity");
        packer.packString("admin");
        packer.packString("authn-secret");
        packer.packString("password");

        ByteBuf byteBuf = Unpooled.wrappedBuffer(packer.toByteArray());

        handler.channelRead(ctx, byteBuf);

        verify(ctx).writeAndFlush(any());
    }

    private void changeConfiguration(Consumer<SecurityChange> changeConsumer) {
        assertThat(securityConfiguration.change(changeConsumer), willCompleteSuccessfully());
        validateConfiguration();
    }

    private void validateConfiguration() {
        ValidationContext<NamedListView<? extends AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                securityConfiguration.value().authentication().providers()
        );

        doReturn(securityConfiguration.value()).when(ctx).getNewRoot(SecurityConfiguration.KEY);

        TestValidationUtil.validate(
                AuthenticationProvidersValidatorImpl.INSTANCE,
                mock(AuthenticationProvidersValidator.class),
                ctx
        );
    }
}
