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

package org.apache.ignite.internal.metrics.exporters.configuration;

import static io.opentelemetry.exporter.otlp.internal.OtlpConfigUtil.PROTOCOL_GRPC;
import static io.opentelemetry.exporter.otlp.internal.OtlpConfigUtil.PROTOCOL_HTTP_PROTOBUF;

import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Endpoint;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.internal.metrics.exporters.otlp.OtlpPushMetricExporter;
import org.apache.ignite.internal.network.configuration.SslConfigurationSchema;
import org.apache.ignite.internal.network.configuration.SslConfigurationValidator;

/**
 * Configuration for OTLP push exporter.
 */
@PolymorphicConfigInstance(OtlpPushMetricExporter.EXPORTER_NAME)
public class OtlpExporterConfigurationSchema extends ExporterConfigurationSchema {
    /** Export period, in milliseconds. */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "period")
    public long periodMillis = 30_000;

    /** String in "host:port" format. */
    @Value
    @Endpoint
    public String endpoint;

    /** OTLP protocol. */
    @OneOf({PROTOCOL_GRPC, PROTOCOL_HTTP_PROTOBUF})
    @Value(hasDefault = true)
    public String protocol = PROTOCOL_GRPC;

    /** Connection headers configuration schema. */
    @NamedConfigValue
    public HeadersConfigurationSchema headers;

    /** SSL configuration schema. */
    @ConfigValue
    @SslConfigurationValidator
    public SslConfigurationSchema ssl;

    /** Method used to compress payloads. */
    @OneOf({"none", "gzip"})
    @Value(hasDefault = true)
    public String compression = "gzip";
}
