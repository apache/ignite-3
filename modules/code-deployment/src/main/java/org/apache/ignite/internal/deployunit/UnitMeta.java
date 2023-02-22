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

package org.apache.ignite.internal.deployunit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.deployment.version.Version;

/**
 * Unit meta data class.
 */
public class UnitMeta {
    /**
     * Unit id.
     */
    private final String id;

    /**
     * Unit version.
     */
    private final Version version;

    /**
     * Unit name.
     */
    private final String unitName;

    /**
     * Consistent ids of nodes with.
     */
    private final List<String> consistentIdLocation = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param id Unit identifier.
     * @param version Unit version.
     * @param unitName Unit name.
     * @param consistentIdLocation Consistent ids of nodes where unit deployed.
     */
    public UnitMeta(String id, Version version, String unitName, List<String> consistentIdLocation) {
        this.id = id;
        this.version = version;
        this.unitName = unitName;
        this.consistentIdLocation.addAll(consistentIdLocation);
    }

    public UnitMeta(String id, String unitName, List<String> consistentIdLocation) {
        this(id, Version.LATEST, unitName, consistentIdLocation);
    }

    public String getId() {
        return id;
    }

    public Version getVersion() {
        return version;
    }

    public String getUnitName() {
        return unitName;
    }

    public List<String> getConsistentIdLocation() {
        return consistentIdLocation;
    }

    public void addConsistentId(String id) {
        consistentIdLocation.add(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnitMeta meta = (UnitMeta) o;
        return Objects.equals(id, meta.id) && Objects.equals(version, meta.version) && Objects.equals(unitName,
                meta.unitName) && Objects.equals(consistentIdLocation, meta.consistentIdLocation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, unitName, consistentIdLocation);
    }
}
