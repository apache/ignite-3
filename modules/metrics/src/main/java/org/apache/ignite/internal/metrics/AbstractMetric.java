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

import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Abstract metric. */
public abstract class AbstractMetric implements Metric {
    /** Metric name. It is local for a particular {@link MetricSet}. */
    private final String name;

    /** Metric description. */
    private final @Nullable String desc;

    /**
     * Constructor.
     *
     * @param name Metric name.
     * @param desc Metric Description.
     */
    public AbstractMetric(String name, @Nullable String desc) {
        assert name != null;
        assert !name.isEmpty();

        this.name = name;
        this.desc = desc;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public @Nullable String description() {
        return desc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractMetric metric = (AbstractMetric) o;

        return name.equals(metric.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return S.toString((Class<? super AbstractMetric>) getClass(), this, "name", name, "description", desc);
    }
}
