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

import static org.apache.ignite.internal.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.internal.client.proto.ClientDataType.BOOLEAN;
import static org.apache.ignite.internal.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATE;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATETIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.internal.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.internal.client.proto.ClientDataType.DURATION;
import static org.apache.ignite.internal.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT16;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT32;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT64;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT8;
import static org.apache.ignite.internal.client.proto.ClientDataType.NUMBER;
import static org.apache.ignite.internal.client.proto.ClientDataType.PERIOD;
import static org.apache.ignite.internal.client.proto.ClientDataType.STRING;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIMESTAMP;
import static org.apache.ignite.internal.client.proto.ClientDataType.UUID;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;

import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;

/**
 * Client binary tuple utils.
 */
public class ClientBinaryTupleUtils {
    /**
     * Reads a binary tuple column and sets the value in the specified tuple.
     *
     * @param reader         Binary tuple reader.
     * @param readerIndex    Column index in the binary tuple.
     * @param tuple          Target tuple.
     * @param columnName     Column name.
     * @param clientDataType Client data type (see {@link ClientDataType}).
     */
    public static void readAndSetColumnValue(
            BinaryTupleReader reader,
            int readerIndex,
            Tuple tuple,
            String columnName,
            int clientDataType,
            int decimalScale) {
        if (reader.hasNullValue(readerIndex)) {
            tuple.set(columnName, null);
            return;
        }

        switch (clientDataType) {
            case INT8:
                tuple.set(columnName, reader.byteValue(readerIndex));
                break;

            case INT16:
                tuple.set(columnName, reader.shortValue(readerIndex));
                break;

            case INT32:
                tuple.set(columnName, reader.intValue(readerIndex));
                break;

            case INT64:
                tuple.set(columnName, reader.longValue(readerIndex));
                break;

            case FLOAT:
                tuple.set(columnName, reader.floatValue(readerIndex));
                break;

            case DOUBLE:
                tuple.set(columnName, reader.doubleValue(readerIndex));
                break;

            case DECIMAL:
                tuple.set(columnName, reader.decimalValue(readerIndex, decimalScale));
                break;

            case UUID:
                tuple.set(columnName, reader.uuidValue(readerIndex));
                break;

            case STRING:
                tuple.set(columnName, reader.stringValue(readerIndex));
                break;

            case BYTES:
                tuple.set(columnName, reader.bytesValue(readerIndex));
                break;

            case BITMASK:
                tuple.set(columnName, reader.bitmaskValue(readerIndex));
                break;

            case NUMBER:
                tuple.set(columnName, reader.numberValue(readerIndex));
                break;

            case DATE:
                tuple.set(columnName, reader.dateValue(readerIndex));
                break;

            case TIME:
                tuple.set(columnName, reader.timeValue(readerIndex));
                break;

            case DATETIME:
                tuple.set(columnName, reader.dateTimeValue(readerIndex));
                break;

            case TIMESTAMP:
                tuple.set(columnName, reader.timestampValue(readerIndex));
                break;

            default:
                throw unsupportedTypeException(clientDataType);
        }
    }

    /**
     * Reads an object from binary tuple at the specified index.
     *
     * @param reader Binary tuple reader.
     * @param index  Starting index in the binary tuple.
     * @return Object.
     */
    public static Object readObject(BinaryTupleReader reader, int index) {
        if (reader.hasNullValue(index)) {
            return null;
        }

        int type = reader.intValue(index);
        int valIdx = index + 2;

        switch (type) {
            case INT8:
                return reader.byteValue(valIdx);

            case INT16:
                return reader.shortValue(valIdx);

            case INT32:
                return reader.intValue(valIdx);

            case INT64:
                return reader.longValue(valIdx);

            case FLOAT:
                return reader.floatValue(valIdx);

            case DOUBLE:
                return reader.doubleValue(valIdx);

            case DECIMAL:
                return reader.decimalValue(valIdx, reader.intValue(index + 1));

            case UUID:
                return reader.uuidValue(valIdx);

            case STRING:
                return reader.stringValue(valIdx);

            case BYTES:
                return reader.bytesValue(valIdx);

            case BITMASK:
                return reader.bitmaskValue(valIdx);

            case DATE:
                return reader.dateValue(valIdx);

            case TIME:
                return reader.timeValue(valIdx);

            case DATETIME:
                return reader.dateTimeValue(valIdx);

            case TIMESTAMP:
                return reader.timestampValue(valIdx);

            case NUMBER:
                return reader.numberValue(valIdx);

            case BOOLEAN:
                return reader.byteValue(valIdx) != 0;

            case DURATION:
                return reader.durationValue(valIdx);

            case PERIOD:
                return reader.periodValue(valIdx);

            default:
                throw unsupportedTypeException(type);
        }
    }

    private static IgniteException unsupportedTypeException(int dataType) {
        return new IgniteException(PROTOCOL_ERR, "Unsupported type: " + dataType);
    }
}
