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

package org.apache.ignite.internal.rest.api.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Objects;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * REST representation of group state.
 */
public class GroupState {
    @IgniteToStringInclude
    private final Collection<String> aliveNodes;

    private final GroupStatus groupStatus;

    @JsonCreator
    public GroupState(
            @JsonProperty("aliveNodes")
            Collection<String> aliveNodes,
            @JsonProperty("groupStatus")
            GroupStatus groupStatus
    ) {
        this.aliveNodes = aliveNodes;
        this.groupStatus = groupStatus;
    }

    @JsonGetter("aliveNodes")
    @JsonInclude
    public Collection<String> aliveNodes() {
        return aliveNodes;
    }

    @JsonGetter("groupStatus")
    public GroupStatus groupStatus() {
        return groupStatus;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupState that = (GroupState) o;
        return Objects.equals(aliveNodes, that.aliveNodes) && groupStatus == that.groupStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(aliveNodes, groupStatus);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
