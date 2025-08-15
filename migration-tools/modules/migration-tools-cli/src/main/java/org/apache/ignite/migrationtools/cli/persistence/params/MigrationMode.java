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

package org.apache.ignite.migrationtools.cli.persistence.params;

/**
 * Allows controlling the error handling policy in case of an irreconcilable mismatch between the GG8 record and the target GG9 Table.
 */
public enum MigrationMode {
    /** Abort the migration. */
    ABORT,
    /** The whole cache record will be ignored (not migrated to the table; lost). */
    SKIP_RECORD,
    /**
     * Any additional columns/fields in the cache record will be ignored (not migrated to the table; lost).
     * The others will be migrated as usual.
     */
    IGNORE_COLUMN,
    /**
     * Any additional columns/fields in the cache record will be serialized to JSON and stored in the `__EXTRA__` column.
     * This is an additional column that the tool adds to the table, it is not a native feature.
     *
     * <p>This mode will allow to leverage the <i>IgniteAdapter.Builder#allowExtraFields</i> feature in the adapter module.</p>
     */
    PACK_EXTRA
}
