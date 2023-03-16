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

package org.apache.ignite.internal.rest.api.metric;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;

/**
 * REST representation of MetricSet.
 */
@Schema(name = "MetricSet")
public class MetricSetDto {
    /** Metric set name. */
    private final String name;

    /** Metrics. */
    private final Collection<MetricDto> metrics;

    /**
     * Constructor.
     *
     * @param name metric name
     * @param metrics metrics
     */
    @JsonCreator
    public MetricSetDto(
            @JsonProperty("name") String name,
            @JsonProperty("metrics") Collection<MetricDto> metrics
    ) {
        this.name = name;
        this.metrics = metrics;
    }

    /**
     * Returns the metric name.
     *
     * @return metric name
     */
    @JsonGetter("name")
    public String name() {
        return name;
    }

    /**
     * Returns metrics.
     *
     * @return metrics
     */
    @JsonGetter("metrics")
    public Collection<MetricDto> metrics() {
        return metrics;
    }
}
