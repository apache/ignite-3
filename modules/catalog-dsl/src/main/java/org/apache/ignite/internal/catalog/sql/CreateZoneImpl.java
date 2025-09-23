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

package org.apache.ignite.internal.catalog.sql;

import static org.apache.ignite.internal.catalog.sql.QueryPartCollection.partsList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.sql.IgniteSql;

class CreateZoneImpl extends AbstractCatalogQuery<Name> {
    private Name zoneName;

    private boolean ifNotExists;

    private final List<Option> withOptions = new ArrayList<>();

    /**
     * Constructor for internal usage.
     *
     * @see CreateFromAnnotationsImpl
     */
    CreateZoneImpl(IgniteSql sql) {
        super(sql);
    }

    @Override
    protected Name result() {
        return zoneName;
    }

    CreateZoneImpl name(String name) {
        Objects.requireNonNull(name, "Zone name must not be null.");

        this.zoneName = Name.simple(name);
        return this;
    }

    CreateZoneImpl ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    CreateZoneImpl replicas(Integer n) {
        Objects.requireNonNull(n, "Replicas count must not be null.");

        withOptions.add(Option.replicas(n));
        return this;
    }

    CreateZoneImpl quorumSize(Integer n) {
        Objects.requireNonNull(n, "Quorum size must not be null.");

        withOptions.add(Option.quorumSize(n));
        return this;
    }

    CreateZoneImpl partitions(Integer n) {
        Objects.requireNonNull(n, "Partitions must not be null.");

        withOptions.add(Option.partitions(n));
        return this;
    }

    CreateZoneImpl distributionAlgorithm(String distributionAlgorithm) {
        Objects.requireNonNull(distributionAlgorithm, "Partition distribution algorithm must not be null.");

        withOptions.add(Option.distributionAlgorithm(distributionAlgorithm));
        return this;
    }

    CreateZoneImpl dataNodesAutoAdjustScaleUp(Integer adjust) {
        Objects.requireNonNull(adjust, "Timeout between node added topology event itself and data nodes switch must not be null.");

        withOptions.add(Option.dataNodesAutoAdjustScaleUp(adjust));
        return this;
    }

    CreateZoneImpl dataNodesAutoAdjustScaleDown(Integer adjust) {
        Objects.requireNonNull(adjust, "Timeout between node left topology event itself and data nodes switch must not be null.");

        withOptions.add(Option.dataNodesAutoAdjustScaleDown(adjust));
        return this;
    }

    CreateZoneImpl filter(String filter) {
        Objects.requireNonNull(filter, "Filter must not be null.");

        withOptions.add(Option.filter(filter));
        return this;
    }

    CreateZoneImpl storageProfiles(String storageProfiles) {
        Objects.requireNonNull(storageProfiles, "Storage profiles must not be null");

        withOptions.add(Option.storageProfiles(storageProfiles));
        return this;
    }

    CreateZoneImpl consistencyMode(String consistencyMode) {
        Objects.requireNonNull(consistencyMode, "Consistency mode must not be null");

        if (!isValidConsistencyMode(consistencyMode)) {
            throw new IllegalArgumentException("Invalid consistency mode: " + consistencyMode);
        }

        withOptions.add(Option.consistencyMode(consistencyMode));
        return this;
    }

    private static boolean isValidConsistencyMode(String consistencyMode) {
        return "HIGH_AVAILABILITY".equals(consistencyMode) || "STRONG_CONSISTENCY".equals(consistencyMode);
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql("CREATE ZONE ");
        if (ifNotExists) {
            ctx.sql("IF NOT EXISTS ");
        }
        ctx.visit(zoneName);

        if (!withOptions.isEmpty()) {
            ctx.sql(" ").sql("WITH ");
            ctx.visit(partsList(withOptions));
        }

        ctx.sql(";");
    }
}
