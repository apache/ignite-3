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
     * Mapping between an existing unit version to a list of nodes (consistent IDs) 
     * where the unit is deployed.
     */
    private final Map<Version, List<String>> versionToConsistentIds;

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param versionToConsistentIds Mapping between an existing unit version to a list of nodes (consistent IDs) where the unit is deployed.
     */
    private UnitStatus(String id, Map<Version, List<String>> versionToConsistentIds) {
        this.id = id;
        this.versionToConsistentIds = Collections.unmodifiableMap(versionToConsistentIds);
    }

    /**
     * Returns a unit identifier.
     *
     * @return Unit identifier.
     */
    public String id() {
        return id;
    }

    /**
     * Returns a unit version.
     *
     * @return Unit version.
     */
    public Set<Version> versions() {
        return Collections.unmodifiableSet(versionToConsistentIds.keySet());
    }

    /**
     * Returns consistent IDs of nodes where the specified unit version is deployed.
     *
     * @param version Unit version.
     * @return Consistent IDs of nodes where the specified unit version is deployed.
     */
    public List<String> consistentIds(Version version) {
        return Collections.unmodifiableList(versionToConsistentIds.get(version));
    }

    /**
     * Builder provider.
     *
     * @param id Identifier of the unit. Not null and not blank.
     * @return Instance of {@link UnitStatusBuilder}.
     */
    public static UnitStatusBuilder builder(String id) {
        Objects.requireNonNull(id);

        return new UnitStatusBuilder(id);
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(id, versionToConsistentIds);
    }

    /** {@inheritDoc} */
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

        /**
         * Constructor.
         *
         * @param id Unit identifier.
         */
        public UnitStatusBuilder(String id) {
            this.id = id;
        }

        /**
         * Appends node consistent ids with provided version.
         *
         * @param version Unit version.
         * @param consistentIds Node consistent IDs.
         * @return {@code this} builder for use in a chained invocation.
         */
        //I don't understand "Appends node consistent ids with provided version".
        public UnitStatusBuilder append(Version version, List<String> consistentIds) {
            versionToConsistentIds.put(version, consistentIds);
            return this;
        }

        /**
         * Builder status method.
         *
         * @return {@link UnitStatus} instance.
         */
        public UnitStatus build() {
            return new UnitStatus(id, versionToConsistentIds);
        }
    }
}

