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

package org.apache.ignite.internal.client.io.netty;

import static org.apache.ignite.lang.ErrorGroups.Client.CLIENT_SSL_CONFIGURATION_ERR;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.client.ClientAuthenticationMode;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.SslConfiguration;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.io.ClientConnection;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.ClientConnectionStateHandler;
import org.apache.ignite.internal.client.io.ClientMessageHandler;
import org.apache.ignite.internal.client.proto.ClientMessageDecoder;
import org.apache.ignite.lang.ErrorGroups.Client;
import org.apache.ignite.lang.IgniteException;

/**
 * Netty-based multiplexer.
 */
public class NettyClientConnectionMultiplexer implements ClientConnectionMultiplexer {
    private final NioEventLoopGroup workerGroup;

    private final Bootstrap bootstrap;

    private final ClientMetricSource metrics;

    /**
     * Constructor.
     */
    public NettyClientConnectionMultiplexer(ClientMetricSource metrics) {
        workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override
    public void start(IgniteClientConfiguration clientCfg) {
        try {
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) clientCfg.connectTimeout());
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) {
                    setupSsl(ch, clientCfg);
                    ch.pipeline().addLast(
                            new ClientMessageDecoder(),
                            new NettyClientMessageHandler());
                }
            });

        } catch (Throwable t) {
            workerGroup.shutdownGracefully();

            throw t;
        }
    }

    private void setupSsl(SocketChannel ch, IgniteClientConfiguration clientCfg) {
        if (clientCfg.ssl() == null || !clientCfg.ssl().enabled()) {
            return;
        }

        try {
            SslConfiguration ssl = clientCfg.ssl();
            SslContextBuilder builder = SslContextBuilder.forClient().trustManager(loadTrustManagerFactory(ssl));

            builder.ciphers(ssl.ciphers());
            ClientAuth clientAuth = toNettyClientAuth(ssl.clientAuthenticationMode());
            if (ClientAuth.NONE != clientAuth) {
                builder.clientAuth(clientAuth).keyManager(loadKeyManagerFactory(ssl));
            }

            SslContext context = builder.build();

            ch.pipeline().addFirst("ssl", context.newHandler(ch.alloc()));
        } catch (NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException | UnrecoverableKeyException e) {
            throw new IgniteException(CLIENT_SSL_CONFIGURATION_ERR, "Client SSL configuration error: " + e.getMessage(), e);
        }
    }

    private static KeyManagerFactory loadKeyManagerFactory(SslConfiguration ssl)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        if (ssl.keyStorePath() != null) {
            char[] ksPassword = ssl.keyStorePassword() == null ? null : ssl.keyStorePassword().toCharArray();
            KeyStore ks = KeyStore.getInstance(new File(ssl.keyStorePath()), ksPassword);
            keyManagerFactory.init(ks, ksPassword);
        } else {
            keyManagerFactory.init(null, null);
        }

        return keyManagerFactory;
    }

    private static TrustManagerFactory loadTrustManagerFactory(SslConfiguration ssl)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        if (ssl.trustStorePath() != null) {
            char[] tsPassword = ssl.trustStorePassword() == null ? null : ssl.trustStorePassword().toCharArray();
            KeyStore ts = KeyStore.getInstance(new File(ssl.trustStorePath()), tsPassword);
            trustManagerFactory.init(ts);
        } else {
            trustManagerFactory.init((KeyStore) null);
        }

        return trustManagerFactory;
    }

    private static ClientAuth toNettyClientAuth(ClientAuthenticationMode igniteClientAuth) {
        switch (igniteClientAuth) {
            case NONE: return ClientAuth.NONE;
            case REQUIRE: return ClientAuth.REQUIRE;
            case OPTIONAL: return ClientAuth.OPTIONAL;
            default: throw new IllegalArgumentException("Client authentication type is not supported");
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        workerGroup.shutdownGracefully();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ClientConnection> openAsync(InetSocketAddress addr,
            ClientMessageHandler msgHnd,
            ClientConnectionStateHandler stateHnd)
            throws IgniteClientConnectionException {
        CompletableFuture<ClientConnection> fut = new CompletableFuture<>();

        ChannelFuture connectFut = bootstrap.connect(addr);

        connectFut.addListener(f -> {
            if (f.isSuccess()) {
                metrics.connectionsEstablishedIncrement();
                metrics.connectionsActiveIncrement();

                ChannelFuture chFut = (ChannelFuture) f;
                chFut.channel().closeFuture().addListener(unused -> metrics.connectionsActiveDecrement());

                NettyClientConnection conn = new NettyClientConnection(chFut.channel(), msgHnd, stateHnd, metrics);

                fut.complete(conn);
            } else {
                Throwable cause = f.cause();

                var err = new IgniteClientConnectionException(
                        Client.CONNECTION_ERR,
                        "Client failed to connect: " + cause.getMessage(),
                        cause);

                fut.completeExceptionally(err);
            }
        });

        return fut;
    }
}
