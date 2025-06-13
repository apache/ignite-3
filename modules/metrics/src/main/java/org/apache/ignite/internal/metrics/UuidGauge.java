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

package org.apache.ignite.internal.metrics;

import java.util.UUID;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * UUID metric based on {@link Supplier}.
 * Use this type of metric with caution, as it is not a scalar metric and might not be supported by all exporters.
 */
public class UuidGauge extends AbstractMetric {
    /** Value supplier. */
    private final Supplier<UUID> val;

    /**
     * Creates a new instance of UuidGauge.
     *
     * @param name Metric name.
     * @param desc Metric Description.
     * @param val Supplier for the metric value.
     */
    public UuidGauge(String name, @Nullable String desc, Supplier<UUID> val) {
        super(name, desc);

        this.val = val;
    }

    /**
     * Value of the metric.
     *
     * @return Value of the metric.
     */
    public UUID value() {
        return val.get();
    }

    @Override
    public @Nullable String getValueAsString() {
        UUID value = value();

        if (value == null) {
            return null;
        }

        return value().toString();
    }
}
