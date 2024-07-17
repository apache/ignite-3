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

import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.metrics.exporters.MetricExporter;

/**
 * Parent for any exporter configuration.
 */
// TODO: IGNITE-17721 at the moment we need to set exporter name twice: here and as the key on named list in MetricConfigurationSchema,
// because we can't use AbstractConfiguration instead of PolymorphicConfig inside of NamedConfigValue
@PolymorphicConfig
public class ExporterConfigurationSchema {
    /**
     * The unique name of appropriate {@link MetricExporter}. It must be the same as {@link MetricExporter#name()}.
     */
    @PolymorphicId
    public String exporterName;

    @InjectedName
    public String name;
}
