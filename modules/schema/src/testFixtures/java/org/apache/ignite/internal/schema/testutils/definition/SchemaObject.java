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

package org.apache.ignite.internal.schema.testutils.definition;

/**
 * Schema object.
 */
public interface SchemaObject {
    /** Default schema name. */
    String DEFAULT_DATABASE_SCHEMA_NAME = "PUBLIC";

    /**
     * Returns name of schema object.
     *
     * @return Object name.
     */
    String name();

    /**
     * Returns database schema name this object belongs to.
     *
     * @return Database schema name.
     */
    default String schemaName() {
        return DEFAULT_DATABASE_SCHEMA_NAME;
    }

    /**
     * Returns object`s canonical name.
     *
     * @return Canonical name.
     */
    default String canonicalName() {
        return schemaName() + '.' + name();
    }
}
