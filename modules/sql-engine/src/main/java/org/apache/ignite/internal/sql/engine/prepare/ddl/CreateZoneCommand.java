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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import org.jetbrains.annotations.Nullable;

/**
 * CREATE ZONE statement.
 */
public class CreateZoneCommand extends AbstractZoneDdlCommand {
    /** Quietly ignore this command if zone is already exists. */
    private boolean ifNotExists;

    /** Replicas number. */
    private Integer replicas;

    /** Number of partitions. */
    private Integer partitions;

    /** Affinity function name. */
    private String affinity;

    /** Data nodes filter expression. */
    private String nodeFilter;

    /** Data nodes auto adjust timeout. */
    private Integer dataNodesAutoAdjust;

    /** Data nodes auto adjust scale up timeout. */
    private Integer dataNodesAutoAdjustScaleUp;

    /** Data nodes auto adjust scale down timeout. */
    private Integer dataNodesAutoAdjustScaleDown;

    /** Storage profiles. */
    private String storageProfiles;

    public String storageProfiles() {
        return storageProfiles;
    }

    public void storageProfiles(String storageProfiles) {
        this.storageProfiles = storageProfiles;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Nullable public Integer replicas() {
        return replicas;
    }

    public void replicas(Integer replicas) {
        this.replicas = replicas;
    }

    @Nullable public Integer partitions() {
        return partitions;
    }

    public void partitions(Integer partitions) {
        this.partitions = partitions;
    }

    @Nullable public String affinity() {
        return affinity;
    }

    public void affinity(String affinity) {
        this.affinity = affinity;
    }

    @Nullable public String nodeFilter() {
        return nodeFilter;
    }

    public void nodeFilter(String nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    @Nullable public Integer dataNodesAutoAdjust() {
        return dataNodesAutoAdjust;
    }

    public void dataNodesAutoAdjust(Integer dataNodesAutoAdjust) {
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
    }

    @Nullable public Integer dataNodesAutoAdjustScaleUp() {
        return dataNodesAutoAdjustScaleUp;
    }

    public void dataNodesAutoAdjustScaleUp(Integer dataNodesAutoAdjustScaleUp) {
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
    }

    @Nullable public Integer dataNodesAutoAdjustScaleDown() {
        return dataNodesAutoAdjustScaleDown;
    }

    public void dataNodesAutoAdjustScaleDown(Integer dataNodesAutoAdjustScaleDown) {
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
    }
}
