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

import static org.apache.ignite.internal.client.proto.ComputeJobType.MARSHALLED_OBJECT_ID;
import static org.apache.ignite.internal.client.proto.ComputeJobType.MARSHALLED_POJO_ID;
import static org.apache.ignite.internal.client.proto.ComputeJobType.MARSHALLED_TUPLE_ID;
import static org.apache.ignite.internal.client.proto.ComputeJobType.NATIVE_ID;
import static org.apache.ignite.internal.client.proto.pojo.PojoConverter.fromTuple;
import static org.apache.ignite.marshalling.Marshaller.tryUnmarshalOrCast;

import java.lang.reflect.InvocationTargetException;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.internal.client.proto.pojo.PojoConversionException;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.jetbrains.annotations.Nullable;

/** Unpacks job arguments and results. */
public final class ClientComputeJobUnpacker {
    /**
     * Unpacks compute job argument. If the marshaller is provided, it will be used to unmarshal the argument. If the marshaller is not
     * provided and the argument is a native column type or a tuple, it will be unpacked accordingly.
     *
     * @param marshaller Marshaller.
     * @param unpacker Unpacker.
     * @return Unpacked argument.
     */
    public static @Nullable Object unpackJobArgument(@Nullable Marshaller<?, byte[]> marshaller, ClientMessageUnpacker unpacker) {
        return unpack(marshaller, unpacker);
    }

    /**
     * Unpacks compute job result. If the marshaller is provided, it will be used to unmarshal the result. If the marshaller is not provided
     * and the result is a native column type or a tuple, it will be unpacked accordingly.
     *
     * @param marshaller Marshaller.
     * @param unpacker Unpacker.
     * @return Unpacked result.
     */
    public static @Nullable Object unpackJobResult(@Nullable Marshaller<?, byte[]> marshaller, ClientMessageUnpacker unpacker) {
        return unpack(marshaller, unpacker);
    }

    /** Underlying byte array expected to be in the following format: | typeId | value |. */
    private static @Nullable Object unpack(@Nullable Marshaller<?, byte[]> marshaller, ClientMessageUnpacker unpacker) {
        if (unpacker.tryUnpackNil()) {
            return null;
        }

        int typeId = unpacker.unpackInt();

        switch (typeId) {
            case NATIVE_ID:
                if (marshaller != null) {
                    throw new UnmarshallingException(
                            "Can not unpack object because the marshaller is provided but the object was packed without marshaller."
                    );
                }

                return unpacker.unpackObjectFromBinaryTuple();
            case MARSHALLED_TUPLE_ID:
                return TupleWithSchemaMarshalling.unmarshal(unpacker.readBinary());

            case MARSHALLED_OBJECT_ID:
                if (marshaller == null) {
                    throw new UnmarshallingException(
                            "Can not unpack object because the marshaller is not provided but the object was packed with marshaller."
                    );
                }
                return tryUnmarshalOrCast(marshaller, unpacker.readBinary());

            case MARSHALLED_POJO_ID:
                return unpackPojo(unpacker);

            default:
                throw new UnmarshallingException("Unsupported compute job type id: " + typeId);
        }
    }

    /** Unpacks compute job argument without marshaller. */
    public static @Nullable Object unpackJobArgumentWithoutMarshaller(ClientMessageUnpacker unpacker) {
        if (unpacker.tryUnpackNil()) {
            return null;
        }

        int typeId = unpacker.unpackInt();

        switch (typeId) {
            case NATIVE_ID:
                return unpacker.unpackObjectFromBinaryTuple();
            case MARSHALLED_TUPLE_ID:
                return TupleWithSchemaMarshalling.unmarshal(unpacker.readBinary());
            case MARSHALLED_OBJECT_ID:
                return unpacker.readBinary();
            case MARSHALLED_POJO_ID:
                return unpackPojo(unpacker);
            default:
                throw new UnmarshallingException("Unsupported compute job type id: " + typeId);
        }
    }

    private static Object unpackPojo(ClientMessageUnpacker unpacker) {
        String className = unpacker.unpackString();

        try {
            Class<?> clazz = Class.forName(className);
            Object obj = clazz.getConstructor().newInstance();

            fromTuple(obj, TupleWithSchemaMarshalling.unmarshal(unpacker.readBinary()));

            return obj;
        } catch (ClassNotFoundException e) {
            throw new UnmarshallingException("Can't find class " + className, e);
        } catch (NoSuchMethodException e) {
            throw new UnmarshallingException("Class " + className + " doesn't have public default constructor", e);
        } catch (InvocationTargetException e) {
            throw new UnmarshallingException("Constructor has thrown an exception", e);
        } catch (InstantiationException e) {
            throw new UnmarshallingException("Can't instantiate an object of class " + className, e);
        } catch (IllegalAccessException e) {
            throw new UnmarshallingException("Constructor is inaccessible", e);
        } catch (PojoConversionException e) {
            throw new UnmarshallingException("Can't unpack object", e);
        }
    }
}
