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

package org.apache.ignite.internal.client.sql;

import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ColumnTypeConverter;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;

/**
 * Client column metadata.
 */
public class ClientColumnMetadata implements ColumnMetadata {
    /** Name. */
    private final String name;

    /** Type. */
    private final ColumnType type;

    /** Nullable flag. */
    private final boolean nullable;

    /** Column precision. */
    private final int precision;

    /** Column scale. */
    private final int scale;

    /** Origin of the result's column. */
    private final ColumnOrigin origin;

    /**
     * Constructor.
     *
     * @param unpacker Unpacker.
     * @param prevColumns Previous columns.
     */
    public ClientColumnMetadata(ClientMessageUnpacker unpacker, List<ColumnMetadata> prevColumns) {
        var propCnt = unpacker.unpackInt();

        assert propCnt >= 6;

        name = unpacker.unpackString();
        nullable = unpacker.unpackBoolean();
        type = ColumnTypeConverter.fromIdOrThrow(unpacker.unpackInt());
        scale = unpacker.unpackInt();
        precision = unpacker.unpackInt();

        if (unpacker.unpackBoolean()) {
            assert propCnt >= 9;

            origin = new ClientColumnOrigin(unpacker, name, prevColumns);
        } else {
            origin = null;
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
        return type.javaClass();
    }

    /** {@inheritDoc} */
    @Override
    public ColumnType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override
    public int scale() {
        return scale;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override
    public ColumnOrigin origin() {
        return origin;
    }
}
