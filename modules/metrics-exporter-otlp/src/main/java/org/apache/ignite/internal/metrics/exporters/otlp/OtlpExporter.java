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
import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import com.google.auto.service.AutoService;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.NoSuchFileException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.metrics.MetricProvider;
import org.apache.ignite.internal.metrics.exporters.PushMetricExporter;
import org.apache.ignite.internal.metrics.exporters.configuration.HeadersView;
import org.apache.ignite.internal.metrics.exporters.configuration.OtlpExporterView;
import org.apache.ignite.internal.network.configuration.SslView;
import org.apache.ignite.internal.network.ssl.KeystoreLoader;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;

/**
 * Otlp(OpenTelemetry) metrics exporter.
 */
@AutoService(org.apache.ignite.internal.metrics.exporters.MetricExporter.class)
public class OtlpExporter extends PushMetricExporter<OtlpExporterView> {
    public static final String EXPORTER_NAME = "otlp";

    private volatile MetricExporter exporter;

    private volatile MetricProducer producer;

    @Override
    public synchronized void start(MetricProvider metricsProvider, OtlpExporterView view, Supplier<UUID> clusterIdSupplier,
            String nodeName) {
        producer = new MetricProducer(metricsProvider, clusterIdSupplier, nodeName);
        exporter = createExporter(view);

        super.start(metricsProvider, view, clusterIdSupplier, nodeName);
    }

    @Override
    public synchronized void stop() {
        super.stop();

        try {
            IgniteUtils.closeAll(exporter);
        } catch (Exception e) {
            log.error("Failed to stop metric exporter: " + name(), e);
        }
    }

    @Override
    public synchronized void reconfigure(OtlpExporterView newVal) {
        super.reconfigure(newVal);

        exporter = createExporter(newVal);
    }

    @Override
    protected long period() {
        return configuration().period();
    }

    @Override
    public void report() {
        exporter.export(producer.collectAllMetrics());
    }

    @Override
    public String name() {
        return EXPORTER_NAME;
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
        String path = "/";

        if (view.protocol().equals(PROTOCOL_HTTP_PROTOBUF)) {
            path = nullOrBlank(uri.getPath()) ? "/v1/metrics" : uri.getPath();
        }

        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), path, null, null).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unexpected exception creating URL.", e);
        }
    }

    private static MetricExporter createExporter(OtlpExporterView view) {
        if (view.protocol().equals(PROTOCOL_GRPC)) {
            OtlpGrpcMetricExporterBuilder builder = OtlpGrpcMetricExporter.builder()
                    .setEndpoint(createEndpoint(view))
                    .setHeaders(headers(view.headers()));

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
                .setHeaders(headers(view.headers()));

        SslView sslView = view.ssl();

        if (sslView.enabled()) {
            TrustManagerFactory trustManagerFactory = createTrustManagerFactory(sslView);
            X509TrustManager trustManager = (X509TrustManager) trustManagerFactory.getTrustManagers()[0];

            builder.setSslContext(createClientSslContext(sslView, trustManagerFactory), trustManager);
        }

        return builder.build();
    }
}
