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

import static java.util.Objects.requireNonNull;

import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.InetAddress;
import zipkin2.codec.BytesEncoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.Sender;

/**
 * Builder class for {@link FileZipkinSpanExporter}.
 */
public class FileZipkinSpanExporterBuilder {
    private String basePath;

    /**
     * Sets the {@link BytesEncoder}, which controls the format used by the {@link Sender}. Defaults
     * to the {@link SpanBytesEncoder#JSON_V2}.
     *
     * @param basePath Base path.
     * @return {@code this} for chaining.
     */
    public FileZipkinSpanExporterBuilder setBasePath(String basePath) {
        requireNonNull(basePath, "basePath");
        this.basePath = basePath;

        return this;
    }

    /**
     * Builds a {@link FileZipkinSpanExporter}.
     *
     * @return a {@code FileZipkinSpanExporter}.
     */
    public SpanExporter build() {
        OtelToZipkinSpanTransformer transformer = OtelToZipkinSpanTransformer.create(InetAddress::getLoopbackAddress);

        return new FileZipkinSpanExporter(basePath, transformer);
    }
}
