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
 * Event that is related to a table.
 */
public abstract class TableEventParameters extends CatalogEventParameters {
    private final int tableId;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param tableId ID of the table to which the event relates.
     */
    public TableEventParameters(long causalityToken, int catalogVersion, int tableId) {
        super(causalityToken, catalogVersion);
        this.tableId = tableId;
    }

    /** Returns an id of a modified table. */
    public int tableId() {
        return tableId;
    }
}
