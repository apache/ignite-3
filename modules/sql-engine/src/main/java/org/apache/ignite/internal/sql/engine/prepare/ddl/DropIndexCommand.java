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

/**
 * DROP INDEX statement.
 */
public class DropIndexCommand implements DdlCommand {
    /** Index name. */
    private String indexName;

    /** If exist flag. */
    private boolean ifExist;

    /** Schema name where this new table will be created. */
    private String schemaName;

    /** Return idx name. */
    public String indexName() {
        return indexName;
    }

    /** Set idx name. */
    public void indexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Quietly ignore this command if index doesn't exist.
     *
     * @return Quietly ignore flag.
     */
    public boolean ifExist() {
        return ifExist;
    }

    /**
     * Quietly ignore this command if index doesn't exist.
     *
     * @param ifExist Exists flag.
     */
    public void ifExist(boolean ifExist) {
        this.ifExist = ifExist;
    }

    /**
     * Return index schema name.
     *
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Sets index schema name.
     *
     * @param schemaName Schema name.
     */
    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
