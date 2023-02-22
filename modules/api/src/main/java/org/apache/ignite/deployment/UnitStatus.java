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

package org.apache.ignite.deployment;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.tostring.S;

/**
 * Deployment unit status.
 */
public class UnitStatus {
    /**
     * Unit identifier.
     */
    private final String id;

    /**
     * Map from existed unit version to list of nodes consistent ids where unit deployed.
     */
    private final Map<Version, List<String>> versionToConsistentIds;

    public UnitStatus(String id, Map<Version, List<String>> versionToConsistentIds) {
        this.id = id;
        this.versionToConsistentIds = Collections.unmodifiableMap(versionToConsistentIds);
    }

    public String getId() {
        return id;
    }

    public Set<Version> versions() {
        return Collections.unmodifiableSet(versionToConsistentIds.keySet());
    }

    public List<String> consistentIds(Version version) {
        return Collections.unmodifiableList(versionToConsistentIds.get(version));
    }

    public static UnitStatusBuilder builder(String id) {
        return new UnitStatusBuilder(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnitStatus that = (UnitStatus) o;
        return Objects.equals(id, that.id) && Objects.equals(versionToConsistentIds, that.versionToConsistentIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, versionToConsistentIds);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Builder for {@link UnitStatus}.
     */
    public static class UnitStatusBuilder {

        private final String id;
        private final Map<Version, List<String>> versionToConsistentIds = new HashMap<>();

        public UnitStatusBuilder(String id) {
            this.id = id;
        }

        public UnitStatusBuilder append(Version version, List<String> consistentIds) {
            versionToConsistentIds.put(version, consistentIds);
            return this;
        }

        public UnitStatus build() {
            return new UnitStatus(id, versionToConsistentIds);
        }
    }
}

