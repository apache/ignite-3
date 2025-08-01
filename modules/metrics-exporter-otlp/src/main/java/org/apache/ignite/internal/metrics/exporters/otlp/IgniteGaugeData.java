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

import static java.util.Collections.singletonList;

import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.PointData;
import java.util.Collection;

/**
 * Wrapper over data point that returns it as a list.
 *
 * @param <T> Data point type.
 */
class IgniteGaugeData<T extends PointData> implements GaugeData<T> {
    private final Collection<T> points;

    IgniteGaugeData(T data) {
        points = singletonList(data);
    }

    @Override
    public Collection<T> getPoints() {
        return points;
    }
}
