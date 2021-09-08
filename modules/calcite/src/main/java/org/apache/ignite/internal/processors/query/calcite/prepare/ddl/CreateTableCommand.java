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

import org.apache.ignite.schema.Column;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE TABLE statement.
 */
public class CreateTableCommand extends AbstractAlterTableCommand {
    /** Backups number for new cache. */
    private Integer replicas;

    /** Number of partitions for the new table. */
    private Integer partitions;

    /** Quietly ignore this command if table already exists. */
    private boolean ifNotExists;

    /** Primary key columns. */
    private List<String> pkCols;

    /** Columns. */
    private List<Column> cols;

    /**
     * @return Number of replicas.
     */
    @Nullable public Integer replicas() {
        return replicas;
    }

    /**
     * @return Number of partitions.
     */
    @Nullable public Integer partitions() {
        return partitions;
    }

    /**
     * @param replicas Number of replicas for new table.
     */
    public void replicas(Integer replicas) {
        this.replicas = replicas;
    }

    /**
     * @param parts Number of partitions for the new table.
     */
    public void partitions(Integer parts) {
        partitions = parts;
    }

    /**
     * @return Columns.
     */
    public List<Column> columns() {
        return cols;
    }

    /**
     * @param cols Columns.
     */
    public void columns(List<Column> cols) {
        this.cols = cols;
    }

    /**
     * @return Quietly ignore this command if table already exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @param ifNotExists Quietly ignore this command if table already exists.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    /**
     * @return Primary key columns.
     */
    public List<String> primaryKeyColumns() {
        return pkCols;
    }

    /**
     * @param pkCols Primary key columns.
     */
    public void primaryKeyColumns(List<String> pkCols) {
        this.pkCols = pkCols;
    }
}
