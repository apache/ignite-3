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

package org.apache.ignite.internal.rest.api.recovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Disaster recovery request to restart partitions. */
@Schema(description = "restart partitions configuration.")
public class RestartPartitionsRequest {
    @Schema(description = "Names specifying nodes to restart partitions. Case-sensitive. "
            + "If empty/omitted, partitions on all nodes are restarted.")
    private final Set<String> nodeNames;

    @Schema(description = "Name of the zone to restart partitions of. Without quotes, case-sensitive.")
    private final String zoneName;

    @Schema(description = "IDs of partitions to restart. If empty/omitted, all partitions will be restarted.")
    private final Set<Integer> partitionIds;

    @Schema(description = "Fully-qualified name of the table to restart partitions of. Without quotes, case-sensitive.")
    private final String tableName;

    @Schema(description = "If partitions data should be destroyed.")
    private final boolean purge;

    /** Constructor. */
    @JsonCreator
    public RestartPartitionsRequest(
            @JsonProperty("nodeNames") @Nullable Set<String> nodeNames,
            @JsonProperty("zoneName") String zoneName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partitionIds") @Nullable Collection<Integer> partitionIds,
            @JsonProperty("purge") boolean purge
    ) {
        Objects.requireNonNull(zoneName);
        Objects.requireNonNull(tableName);

        this.zoneName = zoneName;
        this.tableName = tableName;
        this.purge = purge;
        this.partitionIds = partitionIds == null ? Set.of() : Set.copyOf(partitionIds);
        this.nodeNames = nodeNames == null ? Set.of() : Set.copyOf(nodeNames);
    }

    /** Returns ids of partitions to restart. Empty set means "all partitions". */
    @JsonGetter("partitionIds")
    public Set<Integer> partitionIds() {
        return partitionIds;
    }

    /** Returns names specifying nodes to restart partitions. Empty set means "all nodes". */
    @JsonGetter("nodeNames")
    public Set<String> nodeNames() {
        return nodeNames;
    }

    /** Returns name of the zone to restart partitions of. */
    @JsonGetter("zoneName")
    public String zoneName() {
        return zoneName;
    }

    /** Returns name of the table to restart partitions of. */
    @JsonGetter("tableName")
    public String tableName() {
        return tableName;
    }

    /** If partitions data should be destroyed. */
    @JsonGetter("purge")
    public boolean purge() {
        return purge;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
