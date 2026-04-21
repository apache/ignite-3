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

package org.apache.ignite.internal.binarytuple;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.internal.util.StringUtils;

/**
 * Produces a human-readable string representation of a {@link BinaryTuple} given a {@link StructNativeType} that describes the row schema.
 *
 * <p>Intended for debugging and logging purposes only.
 */
public class BinaryTuplePrinter {
    private final StructNativeType schema;

    /** Constructor. */
    public BinaryTuplePrinter(StructNativeType schema) {
        this.schema = schema;
    }

    /**
     * Renders the given tuple as a string.
     *
     * @param tuple Binary tuple.
     * @return Human-readable representation.
     */
    public String print(BinaryTuple tuple) {
        return printTuple(tuple);
    }

    /**
     * Renders the given tuple as a string.
     *
     * @param tuple Byte buffer representing the binary tuple.
     * @return Human-readable representation.
     */
    public String print(ByteBuffer tuple) {
        return printTuple(new BinaryTupleReader(schema.fieldsCount(), tuple));
    }

    private String printTuple(BinaryTupleReader reader) {
        List<StructNativeType.Field> fields = schema.fields();

        StringBuilder sb = new StringBuilder("BinaryTuple[");

        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }

            StructNativeType.Field field = fields.get(i);
            sb.append(field.name()).append('=');

            if (reader.hasNullValue(i)) {
                sb.append("null");
            } else {
                sb.append(readValue(reader, i, field.type()));
            }
        }

        return sb.append(']').toString();
    }

    private static Object readValue(BinaryTupleReader reader, int index, NativeType type) {
        switch (type.spec()) {
            case BOOLEAN:
                return reader.booleanValue(index);
            case INT8:
                return reader.byteValue(index);
            case INT16:
                return reader.shortValue(index);
            case INT32:
                return reader.intValue(index);
            case INT64:
                return reader.longValue(index);
            case FLOAT:
                return reader.floatValue(index);
            case DOUBLE:
                return reader.doubleValue(index);
            case DECIMAL:
                return reader.decimalValue(index, ((DecimalNativeType) type).scale());
            case UUID:
                return reader.uuidValue(index);
            case STRING:
                return "'" + reader.stringValue(index) + "'";
            case BYTE_ARRAY:
                return StringUtils.toHexString(reader.bytesValue(index));
            case DATE:
                return reader.dateValue(index);
            case TIME:
                return reader.timeValue(index);
            case DATETIME:
                return reader.dateTimeValue(index);
            case TIMESTAMP:
                return reader.timestampValue(index);
            case DURATION:
                return reader.durationValue(index);
            case PERIOD:
                return reader.periodValue(index);
            default:
                return "?(" + type.spec() + ")";
        }
    }
}
