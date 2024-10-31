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
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import java.util.List;

/** REST representation of a list of metric sources for a node. */
@Schema(description = "A list of metric sources for a node.")
public class NodeMetricSources {
    /** Consistent id of the node. */
    private final String node;

    /** List of metric sources. */
    @Schema(description = "Metric sources.", requiredMode = RequiredMode.REQUIRED)
    private final List<MetricSource> sources;

    @JsonCreator
    public NodeMetricSources(@JsonProperty("node") String node, @JsonProperty("sources") List<MetricSource> sources) {
        this.node = node;
        this.sources = sources;
    }

    @JsonGetter("node")
    public String node() {
        return node;
    }

    @JsonGetter("sources")
    public List<MetricSource> sources() {
        return sources;
    }
}
