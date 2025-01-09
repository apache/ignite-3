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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.marshalling.MarshallingException;
import org.apache.ignite.table.Tuple;

public class SharedComputeUtils {
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
}
