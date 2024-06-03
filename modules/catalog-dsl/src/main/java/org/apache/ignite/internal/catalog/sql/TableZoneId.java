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

import org.jetbrains.annotations.Nullable;

/**
 * Composite class for table and zone names.
 */
public class TableZoneId {
    @Nullable
    private final String tableName;

    @Nullable
    private final String zoneName;

    public TableZoneId(@Nullable String tableName, @Nullable String zoneName) {
        this.tableName = tableName;
        this.zoneName = zoneName;
    }

    @Nullable
    public String tableName() {
        return tableName;
    }

    @Nullable
    public String zoneName() {
        return zoneName;
    }
}
