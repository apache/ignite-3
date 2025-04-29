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

/** Disaster recovery request to reset partitions. */
@Schema(description = "Reset zone partitions configuration.")
public class ResetZonePartitionsRequest {
    @Schema(description = "Name of the zone to reset partitions of. Without quotes, case-sensitive.")
    private final String zoneName;

    @Schema(description = "IDs of partitions to reset. All if empty.")
    private final Set<Integer> partitionIds;

    /** Constructor. */
    @JsonCreator
    public ResetZonePartitionsRequest(
            @JsonProperty("zoneName") String zoneName,
            @JsonProperty("partitionIds") @Nullable Collection<Integer> partitionIds
    ) {
        Objects.requireNonNull(zoneName);

        this.zoneName = zoneName;
        this.partitionIds = partitionIds == null ? Set.of() : Set.copyOf(partitionIds);
    }

    /** Returns ids of partitions to reset. */
    @JsonGetter("partitionIds")
    public Set<Integer> partitionIds() {
        return partitionIds;
    }

    /** Returns name of the zone to reset partitions of. */
    @JsonGetter("zoneName")
    public String zoneName() {
        return zoneName;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
