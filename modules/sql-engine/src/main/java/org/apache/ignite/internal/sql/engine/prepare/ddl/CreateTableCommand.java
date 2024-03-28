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

import java.util.List;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE TABLE statement.
 */
public class CreateTableCommand extends AbstractTableDdlCommand {

    /** Primary key index type. */
    public enum PrimaryKeyIndexType {
        SORTED, HASH
    }

    private PrimaryKeyIndexType pkIndexType;

    /** Primary key columns. */
    private List<String> pkCols;

    private List<Collation> pkColsCollations;

    /** Colocation columns. */
    private List<String> colocationCols;

    /** Columns. */
    private List<ColumnDefinition> cols;

    private String zone;

    private String storageProfile;

    /**
     * Get primary key index type.
     */
    public PrimaryKeyIndexType primaryIndexType() {
        return pkIndexType;
    }

    /**
     * Set primary key index type.
     */
    public void primaryIndexType(PrimaryKeyIndexType pkIndexType) {
        this.pkIndexType = pkIndexType;
    }

    /**
     * Get primary key columns.
     */
    public List<String> primaryKeyColumns() {
        return pkCols;
    }

    /**
     * Set primary key columns.
     */
    public void primaryKeyColumns(List<String> pkCols) {
        this.pkCols = pkCols;
    }

    /**
     * Get primary key column collations.
     */
    public List<Collation> primaryKeyCollations() {
        return pkColsCollations;
    }

    /**
     * Set primary key columns collations.
     */
    public void primaryKeyCollations(List<Collation> pkColsCollations) {
        this.pkColsCollations = pkColsCollations;
    }

    /**
     * Get table columns.
     *
     * @return Columns.
     */
    public List<ColumnDefinition> columns() {
        return cols;
    }

    /**
     * Set table columns.
     *
     * @param cols Columns.
     */
    public void columns(List<ColumnDefinition> cols) {
        this.cols = cols;
    }

    /**
     * Set colocation column names.
     *
     * @return Collocation column names.
     */
    @Nullable
    public List<String> colocationColumns() {
        return colocationCols;
    }

    /**
     * Get colocation column names.
     *
     * @param colocationCols Colocation column names.
     */
    public void colocationColumns(List<String> colocationCols) {
        this.colocationCols = colocationCols;
    }

    /**
     * Get zone name.
     */
    @Nullable
    public String zone() {
        return zone;
    }

    /**
     * Set zone name.
     */
    public void zone(String zoneName) {
        this.zone = zoneName;
    }

    /**
     * Get storage profile.
     */
    @Nullable
    public String storageProfile() {
        return storageProfile;
    }

    /**
     * Set storage profile.
     */
    public void storageProfile(String storageProfile) {
        this.storageProfile = storageProfile;
    }
}
