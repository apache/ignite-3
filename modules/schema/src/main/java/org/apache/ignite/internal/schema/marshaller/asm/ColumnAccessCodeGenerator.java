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

package org.apache.ignite.internal.schema.marshaller.asm;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Row access code generator.
 */
public class ColumnAccessCodeGenerator {
    /**
     * @param mode   Read-write mode.
     * @param colIdx Column index in the schema.
     * @return Row column access code generator.
     */
    public static ColumnAccessCodeGenerator createAccessor(BinaryMode mode, int colIdx) {
        switch (mode) {
            case P_BYTE:
                return new ColumnAccessCodeGenerator("byteValue", "appendByte", byte.class, colIdx);
            case P_SHORT:
                return new ColumnAccessCodeGenerator("shortValue", "appendShort", short.class, colIdx);
            case P_INT:
                return new ColumnAccessCodeGenerator("intValue", "appendInt", int.class, colIdx);
            case P_LONG:
                return new ColumnAccessCodeGenerator("longValue", "appendLong", long.class, colIdx);
            case P_FLOAT:
                return new ColumnAccessCodeGenerator("floatValue", "appendFloat", float.class, colIdx);
            case P_DOUBLE:
                return new ColumnAccessCodeGenerator("doubleValue", "appendDouble", double.class, colIdx);
            case BYTE:
                return new ColumnAccessCodeGenerator("byteValueBoxed", "appendByte", Byte.class, byte.class, colIdx);
            case SHORT:
                return new ColumnAccessCodeGenerator("shortValueBoxed", "appendShort", Short.class, short.class, colIdx);
            case INT:
                return new ColumnAccessCodeGenerator("intValueBoxed", "appendInt", Integer.class, int.class, colIdx);
            case LONG:
                return new ColumnAccessCodeGenerator("longValueBoxed", "appendLong", Long.class, long.class, colIdx);
            case FLOAT:
                return new ColumnAccessCodeGenerator("floatValueBoxed", "appendFloat", Float.class, float.class, colIdx);
            case DOUBLE:
                return new ColumnAccessCodeGenerator("doubleValueBoxed", "appendDouble", Double.class, double.class, colIdx);
            case STRING:
                return new ColumnAccessCodeGenerator("stringValue", "appendString", String.class, colIdx);
            case UUID:
                return new ColumnAccessCodeGenerator("uuidValue", "appendUuid", UUID.class, colIdx);
            case BYTE_ARR:
                return new ColumnAccessCodeGenerator("bytesValue", "appendBytes", byte[].class, colIdx);
            case BITSET:
                return new ColumnAccessCodeGenerator("bitmaskValue", "appendBitmask", BitSet.class, colIdx);
            case NUMBER:
                return new ColumnAccessCodeGenerator("numberValue", "appendNumber", BigInteger.class, colIdx);
            case DECIMAL:
                return new ColumnAccessCodeGenerator("decimalValue", "appendDecimal", BigDecimal.class, colIdx);
            case DATE:
                return new ColumnAccessCodeGenerator("dateValue", "appendDate", LocalDate.class, colIdx);
            case TIME:
                return new ColumnAccessCodeGenerator("timeValue", "appendTime", LocalTime.class, colIdx);
            case DATETIME:
                return new ColumnAccessCodeGenerator("dateTimeValue", "appendDateTime", LocalDateTime.class, colIdx);
            case TIMESTAMP:
                return new ColumnAccessCodeGenerator("timestampValue", "appendTimestamp", Instant.class, colIdx);
            default:
                throw new IgniteInternalException("Unsupported binary mode: " + mode);
        }
    }

    /** Reader handle name. */
    private final String readMethodName;

    /** Writer handle name. */
    private final String writeMethodName;

    /** Mapped value type. */
    private final Class<?> mappedType;

    /** Write method argument type. */
    private final Class<?> writeArgType;

    /** Column index in the schema. */
    private final int colIdx;

    /**
     * Constructor.
     *
     * @param readMethodName  Reader handle name.
     * @param writeMethodName Writer handle name.
     * @param mappedType      Mapped value type.
     * @param colIdx          Column index in the schema.
     */
    ColumnAccessCodeGenerator(String readMethodName, String writeMethodName, Class<?> mappedType, int colIdx) {
        this(readMethodName, writeMethodName, mappedType, mappedType, colIdx);
    }

    /**
     * Constructor.
     *
     * @param readMethodName  Reader handle name.
     * @param writeMethodName Writer handle name.
     * @param mappedType      Mapped value type.
     * @param writeArgType    Write method argument type.
     * @param colIdx          Column index in the schema.
     */
    ColumnAccessCodeGenerator(String readMethodName, String writeMethodName, Class<?> mappedType,
            Class<?> writeArgType, int colIdx) {
        this.readMethodName = readMethodName;
        this.writeMethodName = writeMethodName;
        this.colIdx = colIdx;
        this.mappedType = mappedType;
        this.writeArgType = writeArgType;
    }

    /**
     * @return Column index in the schema.
     */
    public int columnIdx() {
        return colIdx;
    }
    
    /**
     * @return Row field read method name.
     */
    public String readMethodName() {
        return readMethodName;
    }
    
    /**
     * @return Row field write method name.
     */
    public String writeMethodName() {
        return writeMethodName;
    }
    
    /**
     * @return Write method arg type.
     */
    public Class<?> writeArgType() {
        return writeArgType;
    }
    
    /**
     * @return Read method return type.
     */
    public Class<?> mappedType() {
        return mappedType;
    }
}
