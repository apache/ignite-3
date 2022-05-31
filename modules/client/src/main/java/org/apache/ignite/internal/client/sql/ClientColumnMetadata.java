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

package org.apache.ignite.internal.client.sql;

import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.sql.ColumnMetadata;

/**
 * Client column metadata.
 */
public class ClientColumnMetadata implements ColumnMetadata {
    /** Name. */
    private final String name;

    /** Value class. */
    private final Class<?> valueClass;

    /** Type. */
    private final Object type;

    /** Nullable flag. */
    private final boolean nullable;

    /**
     * Constructor.
     *
     * @param unpacker Unpacker.
     */
    public ClientColumnMetadata(ClientMessageUnpacker unpacker) {
        try {
            name = unpacker.unpackString();
            nullable = unpacker.unpackBoolean();
            valueClass = Class.forName(unpacker.unpackString());
            type = unpacker.unpackObjectWithType();
        } catch (ClassNotFoundException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
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
