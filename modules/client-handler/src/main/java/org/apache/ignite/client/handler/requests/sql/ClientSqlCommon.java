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

package org.apache.ignite.client.handler.requests.sql;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Period;
import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlColumnType;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;

/**
 * Common SQL request handling logic.
 */
class ClientSqlCommon {
    static void packCurrentPage(ClientMessagePacker out, AsyncResultSet asyncResultSet) {
        ResultSetMetadata meta = asyncResultSet.metadata();
        assert meta != null : "Metadata can't be null when row set is present.";

        List<ColumnMetadata> cols = meta.columns();

        out.packArrayHeader(asyncResultSet.currentPageSize());

        for (SqlRow row : asyncResultSet.currentPage()) {
            for (int i = 0; i < cols.size(); i++) {
                packValue(out, cols.get(i).type(), row, i);
            }
        }

        if (!asyncResultSet.hasMorePages()) {
            asyncResultSet.closeAsync();
        }
    }

    private static void packValue(ClientMessagePacker out, SqlColumnType colType, SqlRow row, int idx) {
        if (row.value(idx) == null) {
            out.packNil();
            return;
        }

        switch (colType) {
            case BOOLEAN:
                out.packBoolean(row.value(idx));
                break;

            case INT8:
                out.packByte(row.byteValue(idx));
                break;

            case INT16:
                out.packShort(row.shortValue(idx));
                break;

            case INT32:
                out.packInt(row.intValue(idx));
                break;

            case INT64:
                out.packLong(row.longValue(idx));
                break;

            case FLOAT:
                out.packFloat(row.floatValue(idx));
                break;

            case DOUBLE:
                out.packDouble(row.doubleValue(idx));
                break;

            case DECIMAL:
                out.packDecimal(row.value(idx));
                break;

            case DATE:
                out.packDate(row.dateValue(idx));
                break;

            case TIME:
                out.packTime(row.timeValue(idx));
                break;

            case DATETIME:
                out.packDateTime(row.datetimeValue(idx));
                break;

            case TIMESTAMP:
                out.packTimestamp(row.timestampValue(idx));
                break;

            case UUID:
                out.packUuid(row.uuidValue(idx));
                break;

            case BITMASK:
                out.packBitSet(row.bitmaskValue(idx));
                break;

            case STRING:
                out.packString(row.stringValue(idx));
                break;

            case BYTE_ARRAY:
                byte[] bytes = row.value(idx);
                out.packBinaryHeader(bytes.length);
                out.writePayload(bytes);
                break;

            case PERIOD:
                Period period = row.value(idx);
                out.packInt(period.getYears());
                out.packInt(period.getMonths());
                out.packInt(period.getDays());
                break;

            case DURATION:
                Duration duration = row.value(idx);
                out.packLong(duration.getSeconds());
                out.packInt(duration.getNano());
                break;

            case NUMBER:
                BigInteger number = row.value(idx);
                out.packBigInteger(number);
                break;

            default:
                throw new UnsupportedOperationException("Unsupported column type: " + colType);
        }
    }
}
