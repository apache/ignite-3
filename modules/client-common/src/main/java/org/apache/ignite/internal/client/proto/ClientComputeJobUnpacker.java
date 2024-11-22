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

import static org.apache.ignite.internal.client.proto.pojo.PojoConverter.fromTuple;
import static org.apache.ignite.marshalling.Marshaller.tryUnmarshalOrCast;

import java.lang.reflect.InvocationTargetException;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.internal.client.proto.pojo.PojoConversionException;
import org.apache.ignite.internal.compute.ComputeJobArgumentHolder;
import org.apache.ignite.internal.compute.ComputeJobType;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.jetbrains.annotations.Nullable;

/** Unpacks job results. */
public final class ClientComputeJobUnpacker {
    /**
     * Unpacks compute job result. If the marshaller is provided, it will be used to unmarshal the result. If the marshaller is not provided
     * and the result class is provided and the result is a tuple, it will be unpacked as a pojo of that class. If the marshaller is not
     * provided and the result is a native column type or a tuple, it will be unpacked accordingly.
     *
     * @param unpacker Unpacker.
     * @param marshaller Marshaller.
     * @param resultClass Result class.
     * @return Unpacked result.
     */
    public static @Nullable Object unpackJobResult(
            ClientMessageUnpacker unpacker,
            @Nullable Marshaller<?, byte[]> marshaller,
            @Nullable Class<?> resultClass
    ) {
        if (unpacker.tryUnpackNil()) {
            return null;
        }

        // Underlying byte array expected to be in the following format: | typeId | value |.
        int typeId = unpacker.unpackInt();
        ComputeJobType type = ComputeJobType.fromId(typeId);
        if (type == null) {
            throw new UnmarshallingException("Unsupported compute job type id: " + typeId);
        }

        switch (type) {
            case NATIVE:
                if (marshaller != null) {
                    throw new UnmarshallingException(
                            "Can not unpack object because the marshaller is provided but the object was packed without marshaller."
                    );
                }

                return unpacker.unpackObjectFromBinaryTuple();
            case MARSHALLED_TUPLE:
                return TupleWithSchemaMarshalling.unmarshal(unpacker.readBinary());

            case MARSHALLED_CUSTOM:
                if (marshaller == null) {
                    throw new UnmarshallingException(
                            "Can not unpack object because the marshaller is not provided but the object was packed with marshaller."
                    );
                }
                return tryUnmarshalOrCast(marshaller, unpacker.readBinary());

            case MARSHALLED_POJO:
                if (resultClass == null) {
                    throw new UnmarshallingException(
                            "Can not unpack object because the pojo class is not provided but the object was packed as pojo. "
                                    + "Provide Job result type in JobDescriptor.resultClass."
                    );
                }
                return unpackPojo(unpacker, resultClass);

            default:
                throw new UnmarshallingException("Unsupported compute job type: " + type);
        }
    }

    private static Object unpackPojo(ClientMessageUnpacker unpacker, Class<?> pojoClass) {
        try {
            Object obj = pojoClass.getConstructor().newInstance();

            fromTuple(obj, TupleWithSchemaMarshalling.unmarshal(unpacker.readBinary()));

            return obj;
        } catch (NoSuchMethodException e) {
            throw new UnmarshallingException("Class " + pojoClass.getName() + " doesn't have public default constructor. "
                    + "Add the default constructor or provide Marshaller for " + pojoClass.getName() + " in JobDescriptor.resultMarshaller",
                    e);
        } catch (InvocationTargetException e) {
            throw new UnmarshallingException("Constructor has thrown an exception", e);
        } catch (InstantiationException e) {
            throw new UnmarshallingException("Can't instantiate an object of class " + pojoClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new UnmarshallingException("Constructor is inaccessible", e);
        } catch (PojoConversionException e) {
            throw new UnmarshallingException("Can't unpack object", e);
        }
    }

    /** Unpacks compute job argument without marshaller. */
    public static @Nullable Object unpackJobArgumentWithoutMarshaller(ClientMessageUnpacker unpacker) {
        if (unpacker.tryUnpackNil()) {
            return null;
        }

        int typeId = unpacker.unpackInt();
        ComputeJobType type = ComputeJobType.fromId(typeId);
        if (type == null) {
            throw new UnmarshallingException("Unsupported compute job type id: " + typeId);
        }

        switch (type) {
            case NATIVE:
                return unpacker.unpackObjectFromBinaryTuple();
            case MARSHALLED_TUPLE: // Fallthrough, both types are unmarshalled just before execution.
            case MARSHALLED_POJO:
                return new ComputeJobArgumentHolder(type, unpacker.readBinary());
            case MARSHALLED_CUSTOM:
                return unpacker.readBinary();
            default:
                throw new UnmarshallingException("Unsupported compute job type: " + type);
        }
    }
}
