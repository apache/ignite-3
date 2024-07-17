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
 * Parameters for events that get produced when a table is renamed.
 */
public class RenameTableEventParameters extends TableEventParameters {
    private final String newTableName;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token for the event.
     * @param catalogVersion New catalog version.
     * @param tableId ID of the altered table.
     * @param newTableName New name of the table.
     */
    public RenameTableEventParameters(long causalityToken, int catalogVersion, int tableId, String newTableName) {
        super(causalityToken, catalogVersion, tableId);

        this.newTableName = newTableName;
    }

    /**
     * Returns the new name of the updated table.
     */
    public String newTableName() {
        return newTableName;
    }
}
