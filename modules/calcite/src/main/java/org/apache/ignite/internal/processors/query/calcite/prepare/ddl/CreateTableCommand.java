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

package org.apache.ignite.internal.processors.query.calcite.prepare.ddl;

import java.util.List;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE TABLE statement.
 */
public class CreateTableCommand extends AbstractDdlCommand {
    /** Replicas number. */
    private Integer replicas;

    /** Number of partitions for the new table. */
    private Integer partitions;

    /** Quietly ignore this command if table already exists. */
    private boolean ifNotExists;

    /** Primary key columns. */
    private List<String> pkCols;

    /** Affinity key columns. */
    private List<String> affCols;

    /** Columns. */
    private List<ColumnDefinition> cols;

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
     * Get quietly ignore flag of this command (ignore if table already exists).
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Set quietly ignore flag to ignore this command if table already exists.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
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
     * Table columns.
     *
     * @return Columns.
     */
    public List<ColumnDefinition> columns() {
        return cols;
    }

    /**
     * Table columns.
     *
     * @param cols Columns.
     */
    public void columns(List<ColumnDefinition> cols) {
        this.cols = cols;
    }

    /**
     * Affinity columns.
     *
     * @return Affinity key columns.
     */
    @Nullable
    public List<String> affColumns() {
        return affCols;
    }

    /**
     * Affinity columns.
     *
     * @param affCols Set affinity key columns.
     */
    public void affColumns(List<String> affCols) {
        this.affCols = affCols;
    }
}
