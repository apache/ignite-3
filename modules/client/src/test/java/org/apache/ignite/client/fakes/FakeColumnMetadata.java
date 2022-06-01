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

package org.apache.ignite.client.fakes;

import java.util.List;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.sql.ColumnMetadata;

/**
 * Fake column meta.
 */
class FakeColumnMetadata implements ColumnMetadata {
    private final String name;

    /**
     * Constructor.
     *
     * @param name Column name.
     */
    FakeColumnMetadata(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> valueClass() {
        return Object.class;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnType type() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public int order() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> origin() {
        return null;
    }
}
