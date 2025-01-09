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
import static org.apache.ignite.internal.compute.PojoConverter.toTuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.MarshallingException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

public class SharedComputeUtils {
    private static final Set<Class<?>> NATIVE_TYPES = Arrays.stream(ColumnType.values())
            .map(ColumnType::javaClass)
            .collect(Collectors.toUnmodifiableSet());

    public static BinaryTupleBuilder writeTupleCollection(Collection<?> col) {
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

    public static List<Tuple> readTupleCollection(ByteBuffer collectionBuf) {
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
    public static <T> ComputeJobDataHolder marshalAndWrapArgOrResult(@Nullable T obj, @Nullable Marshaller<T, byte[]> marshaller) {
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

    private static boolean isNativeType(Class<?> clazz) {
        return NATIVE_TYPES.contains(clazz);
    }
}
