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
    /** Replicas number. */
    private Integer replicas;

    /** Number of partitions for the new table. */
    private Integer partitions;

    private String affinity;

    private String nodeFiler;

    private Integer autoAdjustScaleUp;

    private Integer autoAdjustScaleDown;

    @Nullable
    public Integer replicas() {
        return replicas;
    }

    public void replicas(Integer replicas) {
        this.replicas = replicas;
    }

    @Nullable
    public Integer partitions() {
        return partitions;
    }

    public void partitions(Integer partitions) {
        this.partitions = partitions;
    }

    @Nullable
    public String affinity() {
        return affinity;
    }

    public void affinity(String affinity) {
        this.affinity = affinity;
    }

    @Nullable
    public String nodeFilter() {
        return nodeFiler;
    }

    public void nodeFilter(String nodeFiler) {
        this.nodeFiler = nodeFiler;
    }

    @Nullable
    public Integer autoAdjustScaleUp() {
        return autoAdjustScaleUp;
    }

    public void autoAdjustScaleUp(Integer autoAdjustScaleUp) {
        this.autoAdjustScaleUp = autoAdjustScaleUp;
    }

    @Nullable
    public Integer autoAdjustScaleDown() {
        return autoAdjustScaleDown;
    }

    public void autoAdjustScaleDown(Integer autoAdjustScaleDown) {
        this.autoAdjustScaleDown = autoAdjustScaleDown;
    }
}
