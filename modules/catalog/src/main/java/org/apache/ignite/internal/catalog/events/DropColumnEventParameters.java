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

import java.util.Collection;

/**
 * Drop column event parameters contains descriptors of dropped columns.
 */
public class DropColumnEventParameters extends CatalogEventParameters {

    private final int tableId;
    private final Collection<String> columns;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param tableId An id of table, which columns are dropped from.
     * @param columns Names of columns to drop.
     */
    public DropColumnEventParameters(long causalityToken, int tableId, Collection<String> columns) {
        super(causalityToken);

        this.tableId = tableId;
        this.columns = columns;
    }

    /** Returns table id. */
    public int tableId() {
        return tableId;
    }

    /**
     * Returns names of columns to drop.
     */
    public Collection<String> columns() {
        return columns;
    }
}
