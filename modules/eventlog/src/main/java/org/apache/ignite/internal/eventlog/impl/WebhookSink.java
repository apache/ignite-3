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

package org.apache.ignite.internal.eventlog.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.rest.constants.HttpCode.BAD_GATEWAY;
import static org.apache.ignite.internal.rest.constants.HttpCode.GATEWAY_TIMEOUT;
import static org.apache.ignite.internal.rest.constants.HttpCode.SERVICE_UNAVAILABLE;
import static org.apache.ignite.internal.rest.constants.HttpCode.TOO_MANY_REQUESTS;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.Sink;
import org.apache.ignite.internal.eventlog.config.schema.WebhookSinkRetryPolicyView;
import org.apache.ignite.internal.eventlog.config.schema.WebhookSinkView;
import org.apache.ignite.internal.eventlog.ser.EventSerializer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.configuration.SslView;
import org.apache.ignite.internal.network.ssl.KeystoreLoader;
import org.apache.ignite.internal.rest.constants.MediaType;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.TestOnly;

/**
 * Sink that sends events to an external web server.
 */
class WebhookSink implements Sink<WebhookSinkView> {
    private static final IgniteLogger LOG = Loggers.forClass(WebhookSink.class);

    private static final Set<Integer> RETRYABLE_STATUSES = Set.of(TOO_MANY_REQUESTS.code(), BAD_GATEWAY.code(), SERVICE_UNAVAILABLE.code(),
            GATEWAY_TIMEOUT.code());

    private final WebhookSinkView cfg;

    private final EventSerializer serializer;

    private final Supplier<UUID> clusterIdSupplier;

    private final String nodeName;

    private final HttpClient client;

    private final BlockingQueue<Event> events;

    private final ScheduledExecutorService executorService;

    private volatile long lastSendMillis;

    WebhookSink(WebhookSinkView cfg, EventSerializer serializer, Supplier<UUID> clusterIdSupplier, String nodeName) {
        this.cfg = cfg;
        this.serializer = serializer;
        this.clusterIdSupplier = clusterIdSupplier;
        this.nodeName = nodeName;

        events = new LinkedBlockingQueue<>(cfg.queueSize());

        client = configureClient(cfg);

        executorService = Executors.newSingleThreadScheduledExecutor(
                IgniteThreadFactory.create(nodeName, "eventlog-webhook-sink", LOG)
        );

        executorService.scheduleAtFixedRate(
                this::tryToSendBatch,
                cfg.batchSendFrequencyMillis(),
                cfg.batchSendFrequencyMillis(),
                MILLISECONDS
        );
    }

    @Override
    public void stop() {
        if (executorService != null) {
            if (!events.isEmpty()) {
                executorService.execute(this::tryToSendBatch);
            }

            IgniteUtils.shutdownAndAwaitTermination(executorService, 1, TimeUnit.SECONDS);
        }

        events.clear();
    }

    @Override
    public void write(Event event) {
        while (!events.offer(event)) {
            events.poll();
        }

        if (events.size() >= cfg.batchSize()) {
            executorService.execute(this::tryToSendBatch);
        }
    }

    @TestOnly
    BlockingQueue<Event> getEvents() {
        return events;
    }

    @TestOnly
    long getLastSendMillis() {
        return lastSendMillis;
    }

    private void sendInternal(Collection<Event> batch) {
        WebhookSinkRetryPolicyView rp = cfg.retryPolicy();

        int retryCounter = 0;
        Throwable lastError = null;

        do {
            if (retryCounter > 0) {
                long currentBackoff = (long) (rp.initBackoffMillis() * Math.pow(rp.backoffMultiplier(), retryCounter));
                long backoff = Math.min(currentBackoff, rp.maxBackoffMillis());

                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    break; // Break out and return response or throw
                }
            }

            try {
                HttpResponse<String> res = client.send(createRequest(serializer.serialize(batch)), BodyHandlers.ofString(UTF_8));

                if (RETRYABLE_STATUSES.contains(res.statusCode())) {
                    LOG.trace("Failed to send events to webhook, will retry attempt [name={}, retry={}, statusCode={}]", cfg.endpoint(),
                            retryCounter, res.statusCode());
                } else {
                    LOG.trace("Successfully send events to webhook [name={}, eventCount={}]", cfg.endpoint(), batch.size());

                    lastError = null;

                    break;
                }

            } catch (Throwable e) {
                LOG.trace("Failed to send events to webhook, will retry attempt [name={}, retry={}]", cfg.endpoint(), retryCounter, e);

                lastError = e;
            }
        } while (++retryCounter < rp.maxAttempts());

        if (lastError != null) {
            LOG.warn("Failed to send events to webhook [name={}, eventCount={} lastError={}]", cfg.endpoint(), batch.size(),
                    lastError.getMessage());
        }
    }

    private void tryToSendBatch() {
        if (events.isEmpty()) {
            lastSendMillis = System.currentTimeMillis();

            return;
        }

        Collection<Event> batch = new ArrayList<>(cfg.batchSize());

        while (!events.isEmpty() && (events.size() >= cfg.batchSize()
                || System.currentTimeMillis() - lastSendMillis > cfg.batchSendFrequencyMillis())) {
            events.drainTo(batch, cfg.batchSize());

            sendInternal(batch);

            batch.clear();

            lastSendMillis = System.currentTimeMillis();
        }
    }

    private HttpRequest createRequest(byte[] body) {
        return HttpRequest.newBuilder(URI.create(cfg.endpoint()))
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .header("X-SINK-CLUSTER-ID", String.valueOf(clusterIdSupplier.get()))
                .header("X-SINK-NODE-NAME", nodeName)
                .POST(BodyPublishers.ofByteArray(body))
                .build();
    }

    private static HttpClient configureClient(WebhookSinkView cfg) {
        HttpClient.Builder builder = HttpClient.newBuilder();

        if (cfg.ssl().enabled()) {
            builder.sslContext(createClientSslContext(cfg.ssl(), createTrustManagerFactory(cfg.ssl())));
        }

        return builder.build();
    }

    private static TrustManagerFactory createTrustManagerFactory(SslView ssl) {
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(KeystoreLoader.load(ssl.trustStore()));

            return trustManagerFactory;
        } catch (IOException | GeneralSecurityException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, e);
        }
    }

    private static SSLContext createClientSslContext(SslView ssl, TrustManagerFactory trustManagerFactory) {
        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(KeystoreLoader.load(ssl.keyStore()), ssl.keyStore().password().toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());

            return sslContext;
        } catch (NoSuchFileException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, String.format("File %s not found", e.getMessage()), e);
        } catch (IOException | GeneralSecurityException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, e);
        }
    }
}
