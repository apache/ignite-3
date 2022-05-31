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

package org.apache.ignite.internal.sql.api;

import org.apache.ignite.internal.sql.engine.ResultFieldMetadata;
import org.apache.ignite.sql.ColumnMetadata;

/**
 * Column metadata.
 */
class ColumnMetadataImpl implements ColumnMetadata {
    /** Field meta. */
    private final ResultFieldMetadata fieldMetadata;

    /**
     * Constructor.
     *
     * @param fieldMetadata Field metadata.
     */
    public ColumnMetadataImpl(ResultFieldMetadata fieldMetadata) {
        this.fieldMetadata = fieldMetadata;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return fieldMetadata.name();
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> valueClass() {
        // TODO: IGNITE-16962
        return Object.class;
    }

    /** {@inheritDoc} */
    @Override
    public Object type() {
        // TODO: IGNITE-16962
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return fieldMetadata.isNullable();
    }
}
