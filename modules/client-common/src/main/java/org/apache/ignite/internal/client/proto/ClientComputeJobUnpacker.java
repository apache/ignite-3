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

import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Unpacks job arguments and results. */
public class ClientComputeJobUnpacker {
    private final ClientMessageUnpacker unpacker;

    /**
     * Constructor.
     *
     * @param unpacker that should be closed after usage outside of this class.
     */
    public ClientComputeJobUnpacker(ClientMessageUnpacker unpacker) {
        this.unpacker = unpacker;
    }

    /**
     * Unpacks compute job argument. If the marshaller is provided, it will be used to unmarshal the argument. If the marshaller is not
     * provided and the argument is a native column type or a tuple, it will be unpacked accordingly.
     *
     * @param marshaller Marshaller.
     * @return Unpacked argument.
     */
    public Object unpackJobArgument(@Nullable Marshaller<?, byte[]> marshaller) {
        return unpack(marshaller);
    }

    /**
     * Unpacks compute job result. If the marshaller is provided, it will be used to unmarshal the result. If the marshaller is not provided
     * and the result is a native column type or a tuple, it will be unpacked accordingly.
     *
     * @param marshaller Marshaller.
     * @return Unpacked result.
     */
    public Object unpackJobResult(@Nullable Marshaller<?, byte[]> marshaller) {
        return unpack(marshaller);
    }

    /** Underlying byte array expected to be in the following format: | typeId | value |. */
    private Object unpack(@Nullable Marshaller<?, byte[]> marshaller) {
        int typeId = unpacker.unpackInt();

        var type = ComputeJobType.Type.fromId(typeId);
        switch (type) {
            case NATIVE:
                ColumnType columnType = ColumnType.getById(typeId);
                if (marshaller != null) {
                    if (columnType != ColumnType.BYTE_ARRAY) {
                        throw new IllegalArgumentException("Can not unmarshal object that is not `byte[]`.");
                    }
                    return marshaller.unmarshal((byte[]) unpacker.unpackObjectFromBinaryTuple());
                }

                return unpacker.unpackObjectFromBinaryTuple();
            case MARSHALLED_TUPLE:
                return TupleWithSchemaMarshalling.unmarshal(unpacker.readBinary());
            case MARSHALLED_OBJECT:
                if (marshaller != null) {
                    byte[] bytes = unpacker.readBinary();
                    return marshaller.unmarshal(bytes);
                }
                return unpacker.readBinary();
            default:
                throw new IllegalArgumentException("Unsupported type id: " + typeId);
        }
    }
}
