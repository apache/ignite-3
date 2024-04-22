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

package io.opentelemetry.exporter.zipkin;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import zipkin2.codec.SpanBytesEncoder;

/**
 * {@link SpanExporter} SPI implementation for exporting to file in zipkin format.
 */
public class FileZipkinSpanExporter implements SpanExporter {
    private static final IgniteLogger LOG = Loggers.forClass(FileZipkinSpanExporter.class);

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private final String basePath;
    private final OtelToZipkinSpanTransformer transformer;

    private final Map<String, OutputStream> streams = new LinkedHashMap<>();

    /**
     * Constructor.
     *
     * @param basePath Base path.
     * @param transformer Transformer an OpenTelemetry to SpanData instance.
     */
    FileZipkinSpanExporter(String basePath, OtelToZipkinSpanTransformer transformer) {
        this.basePath = basePath;
        this.transformer = transformer;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        if (isShutdown.get()) {
            return CompletableResultCode.ofFailure();
        }

        for (SpanData spanData : spans) {
            try {
                String traceId = spanData.getTraceId();

                OutputStream out = streams.computeIfAbsent(traceId, this::createTraceFile);

                out.write(SpanBytesEncoder.JSON_V2.encode(transformer.generateSpan(spanData)));

                if (!spanData.getParentSpanContext().isValid()) {
                    closeTraceFile(traceId, streams.remove(spanData.getTraceId()));
                } else {
                    out.write(',');
                }
            } catch (IOException e) {
                LOG.error("Failed to export span to file, will skip it.", e);
            }
        }
        return CompletableResultCode.ofSuccess();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableResultCode shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            LOG.info("Calling shutdown() multiple times.");
        }

        for (Entry<String, OutputStream> entry : streams.entrySet()) {
            try {
                closeTraceFile(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                LOG.warn("Failed close export file for trace: " + entry.getKey(), e);
            }
        }

        streams.clear();

        return CompletableResultCode.ofSuccess();
    }

    private OutputStream createTraceFile(String traceId) {
        try {
            OutputStream out = Files.newOutputStream(Path.of(basePath, traceId  + ".json"));
            out.write('[');

            return out;
        } catch (IOException e) {
            LOG.warn("Failed to create export file for trace: " + traceId, e);

            return OutputStream.nullOutputStream();
        }
    }

    private static void closeTraceFile(String fileName, OutputStream out) {
        try {
            out.write(']');
            out.close();
        } catch (Exception e) {
            LOG.warn("Failed to close export file for trace: " + fileName, e);
        }
    }

    /**
     * Returns a new Builder for {@link FileZipkinSpanExporter}.
     *
     * @return a new {@link FileZipkinSpanExporter}.
     */
    public static FileZipkinSpanExporterBuilder builder() {
        return new FileZipkinSpanExporterBuilder();
    }
}
