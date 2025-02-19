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

package org.apache.ignite.table;

import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class to provide direct access to internals of {@link QualifiedName}.
 */
public final class QualifiedNameHelper {
    /**
     * Return QualifiedName from a given schema and table names.
     *
     * <p>Given names are expected to be normalized, thus it's up to caller to invoke
     * {@link IgniteNameUtils#parseIdentifier(String)} prior passing the names to this method.
     *
     * @param schemaName Normalized schema name.
     * @param tableName Normalized table name.
     * @return Qualified name.
     */
    public static QualifiedName fromNormalized(@Nullable String schemaName, String tableName) {
        assert tableName != null;

        return new QualifiedName(schemaName == null ? SqlCommon.DEFAULT_SCHEMA_NAME : schemaName, tableName);
    }

    private QualifiedNameHelper() {
        // No-op.
    }
}
