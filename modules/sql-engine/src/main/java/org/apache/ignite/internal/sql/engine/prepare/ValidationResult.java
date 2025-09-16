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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.Nullable;

/**
 * ValidationResult holder.
 * Transfer intermediate validation results.
 */
public class ValidationResult {
    private final SqlNode sqlNode;

    @Nullable private final RelDataType dataType;

    @Nullable private final List<List<String>> origins;

    @Nullable private final List<String> aliases;

    /**
     * Constructor.
     *
     * @param sqlNode  Validated SQL node.
     * @param dataType Validated type.
     * @param origins  Type fields provenance.
     * @param aliases  Derived column names.
     */
    ValidationResult(
            SqlNode sqlNode,
            @Nullable RelDataType dataType,
            @Nullable List<List<String>> origins,
            @Nullable List<String> aliases
    ) {
        this.sqlNode = sqlNode;
        this.dataType = dataType;
        this.origins = origins;
        this.aliases = aliases;
    }

    /**
     * Constructor.
     *
     * @param sqlNode  Validated SQL node.
     */
    ValidationResult(SqlNode sqlNode) {
        this.sqlNode = sqlNode;
        this.dataType = null;
        this.origins = null;
        this.aliases = null;
    }

    /**
     * Get validated SQL node.
     */
    public SqlNode sqlNode() {
        return sqlNode;
    }

    /**
     * Get validated type.
     */
    @Nullable public RelDataType dataType() {
        return dataType;
    }

    /**
     * Get type fields provenance.
     */
    @Nullable public List<List<String>> origins() {
        return origins;
    }

    /** Return alternatively derived column names. */
    @Nullable public List<String> aliases() {
        return aliases;
    }
}
