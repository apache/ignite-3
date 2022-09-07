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

import java.util.List;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;

/**
 * CREATE INDEX statement.
 */
public class CreateIndexCommand implements DdlCommand {
    /** Type of the index to create. */
    public enum Type {
        SORTED, HASH
    }

    /** Table name. */
    private String tableName;

    /** Schema name where this new Index will be created. */
    private String schemaName;

    /** Idx name. */
    private String indexName;

    private Type type;

    /** Quietly ignore this command if index already exists. */
    private boolean ifNotExists;

    private List<String> columns;

    private List<Collation> collations;

    /** Return idx name. */
    public String indexName() {
        return indexName;
    }

    /** Set idx name. */
    public void indexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> columns() {
        return columns;
    }

    public void columns(List<String> columns) {
        this.columns = columns;
    }

    public List<Collation> collations() {
        return collations;
    }

    public void collations(List<Collation> collations) {
        this.collations = collations;
    }

    /**
     * Quietly ignore this command if index already exists.
     *
     * @return Quietly ignore flag.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Quietly ignore this command if index already exists.
     *
     * @param ifNotExists Exists flag.
     */
    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void type(Type type) {
        this.type = type;
    }

    public Type type() {
        return type;
    }

    public String tableName() {
        return tableName;
    }

    public void tableName(String tblName) {
        this.tableName = tblName;
    }

    public String schemaName() {
        return schemaName;
    }

    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
