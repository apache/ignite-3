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

package org.apache.ignite.client.handler.requests.sql;

import org.apache.ignite.sql.ColumnMetadata;

/**
 * Client SQL column metadata.
 */
class ClientSqlColumnMetadata implements ColumnMetadata {
    /** */
    private final String name;

    /** */
    private final Class<?> valueClass;

    /** */
    private final Object type;

    /** */
    private final boolean nullable;

    /**
     * Constructor.
     *
     * @param name       Column name.
     * @param valueClass Value class.
     * @param type       Column type.
     * @param nullable   Nullable.
     */
    public ClientSqlColumnMetadata(String name, Class<?> valueClass, Object type, boolean nullable) {
        this.name = name;
        this.valueClass = valueClass;
        this.type = type;
        this.nullable = nullable;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> valueClass() {
        return valueClass;
    }

    /** {@inheritDoc} */
    @Override
    public Object type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return nullable;
    }
}
