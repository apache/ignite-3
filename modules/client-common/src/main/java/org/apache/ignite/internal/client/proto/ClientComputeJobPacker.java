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

import static org.apache.ignite.internal.compute.ComputeJobDataType.MARSHALLED_CUSTOM;
import static org.apache.ignite.internal.compute.ComputeJobDataType.NATIVE;
import static org.apache.ignite.internal.compute.ComputeJobDataType.POJO;
import static org.apache.ignite.internal.compute.ComputeJobDataType.TUPLE;
import static org.apache.ignite.internal.compute.PojoConverter.toTuple;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.internal.compute.ComputeJobResultHolder;
import org.apache.ignite.internal.compute.PojoConversionException;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.MarshallingException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Packs job arguments and results. */
public final class ClientComputeJobPacker {
    private static final Set<Class<?>> NATIVE_TYPES = Arrays.stream(ColumnType.values())
            .map(ColumnType::javaClass)
            .collect(Collectors.toUnmodifiableSet());

    /**
     * Packs compute job argument. If the marshaller is provided, it will be used to marshal the argument. If the marshaller is not provided
     * and the argument is a native column type or a tuple, it will be packed accordingly.
     *
     * @param arg Argument.
     * @param marshaller Marshaller.
     * @param packer Packer.
     * @param <T> Argument type.
     */
    public static <T> void packJobArgument(@Nullable T arg, @Nullable Marshaller<T, byte[]> marshaller, ClientMessagePacker packer) {
        pack(arg, marshaller, packer);
    }

    /**
     * Packs compute job result. If the marshaller is provided, it will be used to marshal the result. If the marshaller is not provided and
     * the result is a native column type or a tuple, it will be packed accordingly.
     *
     * @param res Result.
     * @param marshaller Marshaller.
     * @param packer Packer.
     * @param <T> Result type.
     */
    public static <T> void packJobResult(@Nullable T res, @Nullable Marshaller<T, byte[]> marshaller, ClientMessagePacker packer) {
        pack(res, marshaller, packer);
    }

    /** Packs object in the format: | typeId | value |. */
    private static <T> void pack(@Nullable T obj, @Nullable Marshaller<T, byte[]> marshaller, ClientMessagePacker packer) {
        if (obj == null) {
            packer.packNil();
            return;
        }

        if (obj instanceof ComputeJobResultHolder) {
            ComputeJobResultHolder dataHolder = (ComputeJobResultHolder) obj;
            packer.packInt(dataHolder.type().id());
            packer.packByteBuffer(dataHolder.data());
            return;
        }

        if (marshaller != null) {
            packer.packInt(MARSHALLED_CUSTOM.id());
            byte[] marshalled = marshaller.marshal(obj);

            if (marshalled == null) {
                packer.packNil();
                return;
            }

            packer.packBinary(marshalled);
            return;
        }

        if (obj instanceof Tuple) {
            packer.packInt(TUPLE.id());

            packTuple((Tuple) obj, packer);
            return;
        }

        if (isNativeType(obj.getClass())) {
            packer.packInt(NATIVE.id());

            packer.packObjectAsBinaryTuple(obj);
            return;
        }

        try {
            packer.packInt(POJO.id());

            packTuple(toTuple(obj), packer);
        } catch (PojoConversionException e) {
            throw new MarshallingException("Can't pack object", e);
        }
    }

    private static boolean isNativeType(Class<?> clazz) {
        return NATIVE_TYPES.contains(clazz);
    }

    private static void packTuple(Tuple tuple, ClientMessagePacker packer) {
        packer.packBinary(TupleWithSchemaMarshalling.marshal(tuple));
    }
}
