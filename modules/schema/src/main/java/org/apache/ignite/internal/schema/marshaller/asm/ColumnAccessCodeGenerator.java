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

package org.apache.ignite.internal.schema.marshaller.asm;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.marshaller.BinaryMode;

/**
 * Row access code generator.
 */
public class ColumnAccessCodeGenerator {
    /**
     * Create code generator for column read/write methods.
     *
     * @param mode   Read-write mode.
     * @param colIdx Column index in the schema.
     * @return Row column access code generator.
     */
    public static ColumnAccessCodeGenerator createAccessor(BinaryMode mode, String fieldName, int colIdx) {
        switch (mode) {
            case P_BOOLEAN:
                return new ColumnAccessCodeGenerator("booleanValue", "appendBoolean", boolean.class, fieldName, colIdx);
            case P_BYTE:
                return new ColumnAccessCodeGenerator("byteValue", "appendByte", byte.class, fieldName, colIdx);
            case P_SHORT:
                return new ColumnAccessCodeGenerator("shortValue", "appendShort", short.class, fieldName, colIdx);
            case P_INT:
                return new ColumnAccessCodeGenerator("intValue", "appendInt", int.class, fieldName, colIdx);
            case P_LONG:
                return new ColumnAccessCodeGenerator("longValue", "appendLong", long.class, fieldName, colIdx);
            case P_FLOAT:
                return new ColumnAccessCodeGenerator("floatValue", "appendFloat", float.class, fieldName, colIdx);
            case P_DOUBLE:
                return new ColumnAccessCodeGenerator("doubleValue", "appendDouble", double.class, fieldName, colIdx);
            case BOOLEAN:
                return new ColumnAccessCodeGenerator("booleanValueBoxed", "appendBoolean", Boolean.class, fieldName, colIdx);
            case BYTE:
                return new ColumnAccessCodeGenerator("byteValueBoxed", "appendByte", Byte.class, fieldName, colIdx);
            case SHORT:
                return new ColumnAccessCodeGenerator("shortValueBoxed", "appendShort", Short.class, fieldName, colIdx);
            case INT:
                return new ColumnAccessCodeGenerator("intValueBoxed", "appendInt", Integer.class, fieldName, colIdx);
            case LONG:
                return new ColumnAccessCodeGenerator("longValueBoxed", "appendLong", Long.class, fieldName, colIdx);
            case FLOAT:
                return new ColumnAccessCodeGenerator("floatValueBoxed", "appendFloat", Float.class, fieldName, colIdx);
            case DOUBLE:
                return new ColumnAccessCodeGenerator("doubleValueBoxed", "appendDouble", Double.class, fieldName, colIdx);
            case STRING:
                return new ColumnAccessCodeGenerator("stringValue", "appendString", String.class, fieldName, colIdx);
            case UUID:
                return new ColumnAccessCodeGenerator("uuidValue", "appendUuid", UUID.class, fieldName, colIdx);
            case BYTE_ARR:
                return new ColumnAccessCodeGenerator("bytesValue", "appendBytes", byte[].class, fieldName, colIdx);
            case DECIMAL:
                return new ColumnAccessCodeGenerator("decimalValue", "appendDecimal", BigDecimal.class, fieldName, colIdx);
            case DATE:
                return new ColumnAccessCodeGenerator("dateValue", "appendDate", LocalDate.class, fieldName, colIdx);
            case TIME:
                return new ColumnAccessCodeGenerator("timeValue", "appendTime", LocalTime.class, fieldName, colIdx);
            case DATETIME:
                return new ColumnAccessCodeGenerator("dateTimeValue", "appendDateTime", LocalDateTime.class, fieldName, colIdx);
            case TIMESTAMP:
                return new ColumnAccessCodeGenerator("timestampValue", "appendTimestamp", Instant.class, fieldName, colIdx);
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

    /** Column index in the schema. */
    private final int colIdx;

    /** Field name. */
    private final String filedName;

    /**
     * Constructor.
     *
     * @param readMethodName  Reader handle name.
     * @param writeMethodName Writer handle name.
     * @param mappedType      Mapped value type.
     * @param colIdx          Column index in the schema.
     */
    ColumnAccessCodeGenerator(String readMethodName, String writeMethodName, Class<?> mappedType, String fieldName, int colIdx) {
        this.readMethodName = readMethodName;
        this.writeMethodName = writeMethodName;
        this.colIdx = colIdx;
        this.mappedType = mappedType;
        this.filedName = fieldName;
    }

    /**
     * Gets column index in the schema.
     */
    public int columnIdx() {
        return colIdx;
    }

    /**
     * Gets method name used to read POJO field.
     */
    public String readMethodName() {
        return readMethodName;
    }

    /**
     * Gets method name used to write POJO field.
     */
    public String writeMethodName() {
        return writeMethodName;
    }

    /**
     * Gets read method return type.
     */
    public Class<?> mappedType() {
        return mappedType;
    }

    /**
     * Gets field name.
     */
    public String fieldName() {
        return filedName;
    }
}
