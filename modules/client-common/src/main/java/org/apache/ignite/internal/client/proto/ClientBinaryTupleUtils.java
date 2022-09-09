/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
            int clientDataType) {
        if (reader.hasNullValue(readerIndex)) {
            tuple.set(columnName, null);
            return;
        }

        switch (clientDataType) {
            case ClientDataType.INT8:
                tuple.set(columnName, reader.byteValue(readerIndex));
                break;

            case ClientDataType.INT16:
                tuple.set(columnName, reader.shortValue(readerIndex));
                break;

            case ClientDataType.INT32:
                tuple.set(columnName, reader.intValue(readerIndex));
                break;

            case ClientDataType.INT64:
                tuple.set(columnName, reader.longValue(readerIndex));
                break;

            case ClientDataType.FLOAT:
                tuple.set(columnName, reader.floatValue(readerIndex));
                break;

            case ClientDataType.DOUBLE:
                tuple.set(columnName, reader.doubleValue(readerIndex));
                break;

            case ClientDataType.DECIMAL:
                // TODO IGNITE-17632: Get scale from schema.
                tuple.set(columnName, reader.decimalValue(readerIndex, 100));
                break;

            case ClientDataType.UUID:
                tuple.set(columnName, reader.uuidValue(readerIndex));
                break;

            case ClientDataType.STRING:
                tuple.set(columnName, reader.stringValue(readerIndex));
                break;

            case ClientDataType.BYTES:
                tuple.set(columnName, reader.bytesValue(readerIndex));
                break;

            case ClientDataType.BITMASK:
                tuple.set(columnName, reader.bitmaskValue(readerIndex));
                break;

            case ClientDataType.NUMBER:
                tuple.set(columnName, reader.numberValue(readerIndex));
                break;

            case ClientDataType.DATE:
                tuple.set(columnName, reader.dateValue(readerIndex));
                break;

            case ClientDataType.TIME:
                tuple.set(columnName, reader.timeValue(readerIndex));
                break;

            case ClientDataType.DATETIME:
                tuple.set(columnName, reader.dateTimeValue(readerIndex));
                break;

            case ClientDataType.TIMESTAMP:
                tuple.set(columnName, reader.timestampValue(readerIndex));
                break;

            default:
                throw new IgniteException(PROTOCOL_ERR, "Unsupported type: " + clientDataType);
        }
    }
}
