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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.compute.ComputeJobDataType.MARSHALLED_CUSTOM;
import static org.apache.ignite.internal.compute.ComputeJobDataType.NATIVE;
import static org.apache.ignite.internal.compute.ComputeJobDataType.POJO;
import static org.apache.ignite.internal.compute.ComputeJobDataType.TUPLE;
import static org.apache.ignite.internal.compute.ComputeJobDataType.TUPLE_COLLECTION;
import static org.apache.ignite.internal.compute.PojoConverter.fromTuple;
import static org.apache.ignite.internal.compute.PojoConverter.toTuple;
import static org.apache.ignite.lang.ErrorGroups.Compute.MARSHALLING_TYPE_MISMATCH_ERR;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.MarshallingException;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Compute serialization utils shared between client and embedded APIs.
 */
public class SharedComputeUtils {
    private static final Set<Class<?>> NATIVE_TYPES = Arrays.stream(ColumnType.values())
            .map(ColumnType::javaClass)
            .collect(Collectors.toUnmodifiableSet());

    /**
     * Marshals the job result using either provided marshaller if not {@code null} or depending on the type of the result either as a
     * {@link Tuple}, a native type (see {@link ColumnType}) or a POJO. Wraps the marshalled data with the data type in the
     * {@link ComputeJobDataHolder} to be unmarshalled on the client.
     *
     * @param obj Compute job result.
     * @param marshaller Optional result marshaller.
     *
     * @return Data holder.
     */
    @Nullable
    public static <T> ComputeJobDataHolder marshalArgOrResult(@Nullable T obj, @Nullable Marshaller<T, byte[]> marshaller) {
        if (obj == null) {
            return null;
        }

        if (marshaller != null) {
            byte[] data = marshaller.marshal(obj);
            if (data == null) {
                return null;
            }
            return new ComputeJobDataHolder(MARSHALLED_CUSTOM, data);
        }

        if (obj instanceof Tuple) {
            Tuple tuple = (Tuple) obj;
            return new ComputeJobDataHolder(TUPLE, TupleWithSchemaMarshalling.marshal(tuple));
        }

        if (obj instanceof Collection) {
            Collection<?> col = (Collection<?>) obj;

            // Pack entire collection into a single binary blob, starting with the number of elements (4 bytes, little-endian).
            BinaryTupleBuilder tupleBuilder = SharedComputeUtils.writeTupleCollection(col);

            ByteBuffer binTupleBytes = tupleBuilder.build();

            byte[] resArr = new byte[Integer.BYTES + binTupleBytes.remaining()];
            ByteBuffer resBuf = ByteBuffer.wrap(resArr).order(ByteOrder.LITTLE_ENDIAN);
            resBuf.putInt(col.size());
            resBuf.put(binTupleBytes);

            return new ComputeJobDataHolder(TUPLE_COLLECTION, resArr);
        }


        if (isNativeType(obj.getClass())) {
            // Builder with inline schema.
            // Value is represented by 3 tuple elements: type, scale, value.
            var builder = new BinaryTupleBuilder(3, 3, false);
            ClientBinaryTupleUtils.appendObject(builder, obj);
            return new ComputeJobDataHolder(NATIVE, IgniteUtils.byteBufferToByteArray(builder.build()));
        }

        try {
            // TODO https://issues.apache.org/jira/browse/IGNITE-23320
            Tuple tuple = toTuple(obj);
            return new ComputeJobDataHolder(POJO, TupleWithSchemaMarshalling.marshal(tuple));
        } catch (PojoConversionException e) {
            throw new MarshallingException("Can't pack object: " + obj, e);
        }
    }

    /**
     * Unmarshals the job argument or result.
     *
     * @param holder Data holder.
     * @param marshaller Optional marshaller.
     * @param resultClass Optional result class.
     * @param <T> Type of the object.
     * @return Unmarshalled object.
     */
    public static <T> @Nullable T unmarshalArgOrResult(
            @Nullable ComputeJobDataHolder holder,
            @Nullable Marshaller<?, byte[]> marshaller,
            @Nullable Class<?> resultClass) {
        if (holder == null) {
            return null;
        }

        ComputeJobDataType type = holder.type();
        if (type != MARSHALLED_CUSTOM && marshaller != null) {
            throw new ComputeException(
                    MARSHALLING_TYPE_MISMATCH_ERR,
                    "Marshaller is defined on the server, but the argument was not marshalled on the client. "
                            + "If you want to use default marshalling strategy, "
                            + "then you should not define your marshaller in the job. "
                            + "If you would like to use your own marshaller, then double-check "
                            + "that both of them are defined in the client and in the server."
            );
        }

        switch (type) {
            case NATIVE: {
                var reader = new BinaryTupleReader(3, holder.data());
                return (T) ClientBinaryTupleUtils.readObject(reader, 0);
            }

            case TUPLE: // Fallthrough TODO https://issues.apache.org/jira/browse/IGNITE-23320
            case POJO:
                Tuple tuple = TupleWithSchemaMarshalling.unmarshal(holder.data());
                if (resultClass != null && resultClass != Tuple.class) {
                    return (T) unmarshalPojo(resultClass, tuple);
                }
                return (T) tuple;

            case MARSHALLED_CUSTOM:
                if (marshaller == null) {
                    throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR, "Marshaller should be defined on the client");
                }
                try {
                    return (T) marshaller.unmarshal(holder.data());
                } catch (Exception ex) {
                    throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR, "Exception in user-defined marshaller", ex);
                }

            case TUPLE_COLLECTION:
                return (T) readTupleCollection(ByteBuffer.wrap(holder.data()).order(ByteOrder.LITTLE_ENDIAN));

            default:
                throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR, "Unexpected job argument type: " + type);
        }
    }

    /**
     * Unmarshals a POJO from the tuple.
     *
     * @param pojoType POJO type.
     * @param input Tuple.
     * @return Unmarshalled POJO.
     */
    public static Object unmarshalPojo(Class<?> pojoType, Tuple input) {
        try {
            Object obj = pojoType.getConstructor().newInstance();

            fromTuple(obj, input);

            return obj;
        } catch (NoSuchMethodException e) {
            throw new UnmarshallingException("Class " + pojoType.getName() + " doesn't have public default constructor. "
                    + "Add the constructor or define argument marshaller in the compute job.", e);
        } catch (InvocationTargetException e) {
            throw new UnmarshallingException("Constructor has thrown an exception", e);
        } catch (InstantiationException e) {
            throw new UnmarshallingException("Can't instantiate an object of class " + pojoType.getName(), e);
        } catch (IllegalAccessException e) {
            throw new UnmarshallingException("Constructor is inaccessible", e);
        } catch (PojoConversionException e) {
            throw new UnmarshallingException("Can't unpack object", e);
        }
    }

    private static boolean isNativeType(Class<?> clazz) {
        return NATIVE_TYPES.contains(clazz);
    }

    private static BinaryTupleBuilder writeTupleCollection(Collection<?> col) {
        BinaryTupleBuilder builder = new BinaryTupleBuilder(col.size());

        for (Object el : col) {
            if (el == null) {
                builder.appendNull();
                continue;
            }

            if (!(el instanceof Tuple)) {
                throw new MarshallingException("Can't pack collection: expected Tuple, but got " + el.getClass(), null);
            }

            builder.appendBytes(TupleWithSchemaMarshalling.marshal((Tuple) el));
        }

        return builder;
    }

    private static List<Tuple> readTupleCollection(ByteBuffer collectionBuf) {
        int count = collectionBuf.getInt();
        BinaryTupleReader reader = new BinaryTupleReader(count, collectionBuf.slice().order(ByteOrder.LITTLE_ENDIAN));

        List<Tuple> res = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ByteBuffer elementBytes = reader.bytesValueAsBuffer(i);

            if (elementBytes == null) {
                res.add(null);
                continue;
            }

            res.add(TupleWithSchemaMarshalling.unmarshal(elementBytes));
        }

        return res;
    }
}
