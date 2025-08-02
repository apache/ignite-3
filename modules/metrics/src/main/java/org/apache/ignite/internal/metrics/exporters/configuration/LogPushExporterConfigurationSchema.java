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

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.metrics.exporters.log.LogPushExporter;

/**
 * Configuration for log push exporter.
 */
@PolymorphicConfigInstance(LogPushExporter.EXPORTER_NAME)
public class LogPushExporterConfigurationSchema extends ExporterConfigurationSchema {
    /** Export period, in milliseconds. */
    @Value(hasDefault = true)
    @PublicName(legacyNames = "period")
    public long periodMillis = 30_000;

    /** Whether to print metrics of one metric source in single log line. */
    @Value(hasDefault = true)
    public boolean oneLinePerMetricSource = true;

    /**
     * List of enabled metric sources. If not empty, metric sources that are not enumerated will not be printed.
     * Wildcard '*' can be used in the end of each item. Some metrics are logged by default. To disable it, specify the empty list here
     * explicitly. To print all metrics, include single string '*'.
     */
    @Value(hasDefault = true)
    public String[] enabledMetrics = {
            "metastorage",
            "placement-driver",
            "resource.vacuum"
    };
}
