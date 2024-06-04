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

import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import com.google.auto.service.AutoService;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.traces.ConfigurableSpanExporterProvider;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * Provider class for {@link FileZipkinSpanExporter}.
 */
@AutoService(ConfigurableSpanExporterProvider.class)
public class FileZipkinSpanExporterProvider implements ConfigurableSpanExporterProvider {
    /** {@inheritDoc} */
    @Override
    public SpanExporter createExporter(ConfigProperties config) {
        FileZipkinSpanExporterBuilder builder = FileZipkinSpanExporter.builder();

        String basePath = config.getString("otel.exporter.file-zipkin.base-path");
        if (!nullOrBlank(basePath)) {
            builder.setBasePath(basePath);
        }

        return builder.build();
    }

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return "file-zipkin";
    }
}
