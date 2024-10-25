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

package org.apache.ignite.internal.rest.api.recovery.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Request to reset cluster. */
@Schema(description = "Reset cluster.")
public class ResetClusterRequest {
    @Schema(description = "Names of the proposed CMG nodes. Optional if the MG group is being repaired. If not specified, the current "
            + "CMG nodes are used.")
    @IgniteToStringInclude
    private final @Nullable List<String> cmgNodeNames;

    @Schema(description = "Number of nodes in the voting member set of the Metastorage RAFT group.")
    @IgniteToStringInclude
    private final @Nullable Integer metastorageReplicationFactor;

    /** Constructor. */
    @JsonCreator
    public ResetClusterRequest(
            @JsonProperty("cmgNodeNames") @Nullable List<String> cmgNodeNames,
            @JsonProperty("metastorageReplicationFactor") @Nullable Integer metastorageReplicationFactor
    ) {
        this.cmgNodeNames = cmgNodeNames == null ? null : List.copyOf(cmgNodeNames);

        this.metastorageReplicationFactor = metastorageReplicationFactor;
    }

    /** Returns names of the proposed CMG nodes. */
    @JsonGetter("cmgNodeNames")
    public @Nullable List<String> cmgNodeNames() {
        return cmgNodeNames;
    }

    /** Returns number of nodes in the Raft voting member set for Metastorage. */
    @JsonGetter("metastorageReplicationFactor")
    public @Nullable Integer metastorageReplicationFactor() {
        return metastorageReplicationFactor;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /** If Metastorage repair should be done along with cluster reset. */
    public boolean metastorageRepairRequested() {
        return metastorageReplicationFactor != null;
    }
}
