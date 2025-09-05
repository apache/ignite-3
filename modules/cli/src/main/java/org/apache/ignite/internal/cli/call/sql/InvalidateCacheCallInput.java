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

package org.apache.ignite.internal.cli.call.sql;

import java.util.List;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.jetbrains.annotations.Nullable;

/**
 * Input for {@link InvalidatePlannerCacheCall}.
 */
public class InvalidateCacheCallInput implements CallInput {
    /** Cluster url. */
    private final String clusterUrl;

    /** Tables filter. */
    @Nullable
    private final List<String> tables;

    @Nullable
    private final List<String> targetNodes;

    /** Returns {@link InvalidateCacheCallInput} with specified arguments. */
    public static InvalidateCacheCallInput of(String clusterUrl, @Nullable List<String> tables, @Nullable List<String> targetNodes) {
        return new InvalidateCacheCallInput(clusterUrl, tables, targetNodes);
    }

    private InvalidateCacheCallInput(String clusterUrl, @Nullable List<String> tables, @Nullable List<String> targetNodes) {
        this.clusterUrl = clusterUrl;
        this.tables = tables;
        this.targetNodes = targetNodes;
    }

    /** Cluster url. */
    public String clusterUrl() {
        return clusterUrl;
    }

    /** Returns names specifying nodes to restart partitions. Empty/null means "all nodes". */
    @Nullable
    public List<String> targetNodes() {
        return targetNodes;
    }

    /** Return tables that SQL plans contains any of these tables should be invalidated. */
    @Nullable
    public List<String> getTables() {
        return tables;
    }
}
