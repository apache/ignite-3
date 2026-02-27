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

package org.apache.ignite.internal.metrics.exporters.otlp;

import static io.opentelemetry.exporter.otlp.internal.OtlpConfigUtil.PROTOCOL_GRPC;
import static io.opentelemetry.exporter.otlp.internal.OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF;
import static io.opentelemetry.sdk.common.export.MemoryMode.REUSABLE_DATA;
import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.exporters.configuration.HeadersView;
import org.apache.ignite.internal.metrics.exporters.configuration.OtlpExporterView;
import org.apache.ignite.internal.network.configuration.SslView;
import org.apache.ignite.internal.network.ssl.KeystoreLoader;
import org.apache.ignite.internal.util.Lazy;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A reporter which outputs measurements to a {@link MetricExporter}.
 */
class MetricReporter implements AutoCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(MetricReporter.class);

    private final Map<String, Collection<MetricData>> metricsBySet = new ConcurrentHashMap<>();
    private final Lazy<Resource> resource;

    private MetricExporter exporter;

    MetricReporter(OtlpExporterView view, Supplier<UUID> clusterIdSupplier, String nodeName) {
        this.exporter = createExporter(view);

        this.resource = new Lazy<>(() -> Resource.builder()
                .put("service.name", clusterIdSupplier.get().toString())
                .put("service.instance.id", nodeName)
                .build());
    }

    void addMetricSet(MetricSet metricSet) {
        Collection<MetricData> metrics0 = new ArrayList<>();

        InstrumentationScopeInfo scope = InstrumentationScopeInfo.builder(metricSet.name())
                .build();

        for (Metric metric : metricSet) {
            MetricData metricData = toMetricData(resource, scope, metric);

            if (metricData != null) {
                metrics0.add(metricData);
            }
        }

        metricsBySet.put(metricSet.name(), metrics0);
    }

    void removeMetricSet(String metricSetName) {
        metricsBySet.remove(metricSetName);
    }

    void report() {
        if (!metricsBySet.isEmpty()) {
            Collection<MetricData> allMetrics = metricsBySet.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            exporter.export(allMetrics);
        }
    }

    @Override
    public void close() throws Exception {
        exporter.close();
    }

    @TestOnly
    void exporter(MetricExporter exporter) {
        this.exporter = exporter;
    }

    private static Supplier<Map<String, String>> headers(NamedListView<? extends HeadersView> headers) {
        return () -> headers.stream().collect(Collectors.toUnmodifiableMap(HeadersView::name, HeadersView::header));
    }

    /** Create client SSL context. */
    private static TrustManagerFactory createTrustManagerFactory(SslView ssl) {
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(KeystoreLoader.load(ssl.trustStore()));

            return trustManagerFactory;
        } catch (IOException | GeneralSecurityException e) {
            throw new IgniteException(Common.SSL_CONFIGURATION_ERR, e);
        }
    }

    /** Create client SSL context. */
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

    private static String createEndpoint(OtlpExporterView view) {
        URI uri = URI.create(view.endpoint());
        StringBuilder sb = new StringBuilder();

        if (view.protocol().equals(PROTOCOL_HTTP_PROTOBUF)) {
            String basePath = uri.getPath();

            if (!nullOrBlank(basePath)) {
                sb.append(basePath);
            }

            if (!basePath.endsWith("v1/metrics")) {
                if (!basePath.endsWith("/")) {
                    sb.append('/');
                }

                sb.append("v1/metrics");
            }
        } else {
            sb.append('/');
        }

        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), sb.toString(), null, null).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unexpected exception creating URL.", e);
        }
    }

    private static MetricExporter createExporter(OtlpExporterView view) {
        if (view.protocol().equals(PROTOCOL_GRPC)) {
            OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(createEndpoint(view))
                    .setHeaders(headers(view.headers()))
                    .setCompression(view.compression())
                    .setMemoryMode(REUSABLE_DATA);

            SslView sslView = view.ssl();

            if (sslView.enabled()) {
                TrustManagerFactory trustManagerFactory = createTrustManagerFactory(sslView);
                X509TrustManager trustManager = (X509TrustManager) trustManagerFactory.getTrustManagers()[0];

                builder.setSslContext(createClientSslContext(sslView, trustManagerFactory), trustManager);
            }

            return builder.build();
        }

        OtlpHttpMetricExporterBuilder builder = OtlpHttpMetricExporter.builder()
                .setEndpoint(createEndpoint(view))
                .setHeaders(headers(view.headers()))
                .setCompression(view.compression())
                .setMemoryMode(REUSABLE_DATA);

        SslView sslView = view.ssl();

        if (sslView.enabled()) {
            TrustManagerFactory trustManagerFactory = createTrustManagerFactory(sslView);
            X509TrustManager trustManager = (X509TrustManager) trustManagerFactory.getTrustManagers()[0];

            builder.setSslContext(createClientSslContext(sslView, trustManagerFactory), trustManager);
        }

        return builder.build();
    }

    private static @Nullable MetricData toMetricData(Lazy<Resource> resource, InstrumentationScopeInfo scope, Metric metric) {
        if (metric instanceof IntMetric) {
            return new IgniteIntMetricData(resource, scope, (IntMetric) metric);
        }

        if (metric instanceof LongMetric) {
            return new IgniteLongMetricData(resource, scope, (LongMetric) metric);
        }

        if (metric instanceof DoubleMetric) {
            return new IgniteDoubleMetricData(resource, scope, (DoubleMetric) metric);
        }

        if (metric instanceof DistributionMetric) {
            return new IgniteDistributionMetricData(resource, scope, (DistributionMetric) metric);
        }

        LOG.debug("Unknown metric class for export " + metric.getClass());

        return null;
    }
}
