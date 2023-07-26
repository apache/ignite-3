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

package org.apache.ignite.internal.catalog.events;

/**
 * Drop index event parameters that contains an id of dropped index.
 */
public class DropIndexEventParameters extends CatalogEventParameters {

    private final int indexId;

    private final int tableId;

    private final String tableName;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param indexId An id of dropped index.
     * @param tableId Table ID for which the index was removed.
     * @param tableName Table name for which the index was removed.
     */
    public DropIndexEventParameters(long causalityToken, int catalogVersion, int indexId, int tableId, String tableName) {
        super(causalityToken, catalogVersion);

        this.indexId = indexId;
        this.tableId = tableId;
        this.tableName = tableName;
    }

    /** Returns an id of dropped index. */
    public int indexId() {
        return indexId;
    }

    /** Returns table ID for which the index was removed. */
    public int tableId() {
        return tableId;
    }

    /**
     * Returns table name for with the index was removed.
     *
     * <p>NOTE: This is a temporary solution to avoid getting the table id from the configuration if the table has been dropped.
     */
    @Deprecated(forRemoval = true)
    // TODO: IGNITE-19499 Should be removed and only the table id from the catalog should be used
    public String tableName() {
        return tableName;
    }
}
