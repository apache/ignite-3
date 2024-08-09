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

package org.apache.ignite.internal.client.proto;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Packs job arguments and results. */
public class ClientComputeJobPacker {
    private final ClientMessagePacker packer;

    /**
     * Constructor.
     *
     * @param packer that should be closed after usage outside of this class.
     */
    public ClientComputeJobPacker(ClientMessagePacker packer) {
        this.packer = packer;
    }

    /**
     * Packs compute job argument. If the marshaller is provided, it will be used to marshal the argument. If the marshaller is not provided
     * and the argument is a native column type or a tuple, it will be packed accordingly.
     *
     * @param arg Argument.
     * @param marshaller Marshaller.
     * @param <T> Argument type.
     */
    public <T> void packJobArgument(@Nullable T arg, @Nullable Marshaller<T, byte[]> marshaller) {
        pack(arg, marshaller);
    }

    /**
     * Packs compute job result. If the marshaller is provided, it will be used to marshal the result. If the marshaller is not provided and
     * the result is a native column type or a tuple, it will be packed accordingly.
     *
     * @param res Result.
     * @param marshaller Marshaller.
     * @param <T> Result type.
     */
    public <T> void packJobResult(@Nullable T res, @Nullable Marshaller<T, byte[]> marshaller) {
        pack(res, marshaller);
    }

    /** Packs object in the format: | typeId | value |. */
    private <T> void pack(@Nullable T obj, @Nullable Marshaller<T, byte[]> marshaller) {
        if (marshaller != null) {
            packer.packInt(ComputeJobType.MARSHALLED_OBJECT_ID);
            byte[] marshalled = marshaller.marshal(obj);

            if (marshalled == null) {
                packer.packNil();
                return;
            }

            packer.packByteBuffer(ByteBuffer.wrap(marshalled));
            return;
        }

        if (obj instanceof Tuple) {
            byte[] marshalledTuple = TupleWithSchemaMarshalling.marshal((Tuple) obj);

            packer.packInt(ComputeJobType.MARSHALLED_TUPLE_ID);

            if (marshalledTuple == null) {
                packer.packNil();
                return;
            }

            packer.packByteBuffer(ByteBuffer.wrap(marshalledTuple));
            return;
        }

        if (obj == null) {
            packer.packInt(ColumnType.NULL.id());
            packer.packNil();
            return;
        }

        var type = getColumnTypeFrom(obj);
        assert type != null;

        packer.packInt(type.id());

        packer.packObjectAsBinaryTuple(obj);
    }

    private static @Nullable ColumnType getColumnTypeFrom(Object obj) {
        if (obj instanceof Boolean) {
            return ColumnType.BOOLEAN;
        } else if (obj instanceof Byte) {
            return ColumnType.INT8;
        } else if (obj instanceof Short) {
            return ColumnType.INT16;
        } else if (obj instanceof Integer) {
            return ColumnType.INT32;
        } else if (obj instanceof Long) {
            return ColumnType.INT64;
        } else if (obj instanceof Float) {
            return ColumnType.FLOAT;
        } else if (obj instanceof Double) {
            return ColumnType.DOUBLE;
        } else if (obj instanceof BigDecimal) {
            return ColumnType.DECIMAL;
        } else if (obj instanceof UUID) {
            return ColumnType.UUID;
        } else if (obj instanceof String) {
            return ColumnType.STRING;
        } else if (obj instanceof byte[]) {
            return ColumnType.BYTE_ARRAY;
        } else if (obj instanceof LocalDate) {
            return ColumnType.DATE;
        } else if (obj instanceof LocalTime) {
            return ColumnType.TIME;
        } else if (obj instanceof LocalDateTime) {
            return ColumnType.DATETIME;
        } else if (obj instanceof Instant) {
            return ColumnType.TIMESTAMP;
        } else if (obj instanceof Duration) {
            return ColumnType.DURATION;
        } else if (obj instanceof Period) {
            return ColumnType.PERIOD;
        } else {
            return null;
        }
    }

}
