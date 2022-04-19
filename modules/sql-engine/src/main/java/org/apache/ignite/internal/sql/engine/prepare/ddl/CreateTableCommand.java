/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE TABLE statement.
 */
public class CreateTableCommand extends AbstractTableDdlCommand {
    /** Replicas number. */
    private Integer replicas;

    /** Number of partitions for the new table. */
    private Integer partitions;

    /** Primary key columns. */
    private List<String> pkCols;

    /** Colocation columns. */
    private List<String> colocationCols;

    /** Columns. */
    private List<ColumnDefinition> cols;

    private String dataStorage;

    @Nullable
    private Map<String, Object> dataStorageOptions;

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
     * Get replicas count.
     */
    @Nullable
    public Integer replicas() {
        return replicas;
    }

    /**
     * Set replicas count.
     */
    @Nullable
    public void replicas(int repl) {
        replicas = repl;
    }

    /**
     * Get partitions count.
     */
    @Nullable
    public Integer partitions() {
        return partitions;
    }

    /**
     * Set partitions count.
     */
    public void partitions(Integer parts) {
        partitions = parts;
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
     * Returns data storage.
     */
    public String dataStorage() {
        return dataStorage;
    }

    /**
     * Sets data storage.
     *
     * @param dataStorage Data storage.
     */
    public void dataStorage(String dataStorage) {
        this.dataStorage = dataStorage;
    }

    /**
     * Returns data storage options.
     */
    public Map<String, Object> dataStorageOptions() {
        return dataStorageOptions == null ? Map.of() : dataStorageOptions;
    }

    /**
     * Adds data storage option.
     *
     * @param name Option name.
     * @param value Option value.
     */
    public void addDataStorageOption(String name, Object value) {
        if (dataStorageOptions == null) {
            dataStorageOptions = new HashMap<>();
        }

        dataStorageOptions.put(name, value);
    }
}
