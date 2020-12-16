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

package org.apache.ignite.internal.schema.marshaller.generator;

import com.squareup.javapoet.CodeBlock;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;

/**
 * Tuple access code generator.
 */
public class TupleColumnAccessCodeGenerator {
    /** Tuple method handler. */
    public static final MethodHandle READ_BYTE;
    /** Tuple method handler. */
    public static final MethodHandle READ_SHORT;
    /** Tuple method handler. */
    public static final MethodHandle READ_INT;
    /** Tuple method handler. */
    public static final MethodHandle READ_LONG;
    /** Tuple method handler. */
    public static final MethodHandle READ_FLOAT;
    /** Tuple method handler. */
    public static final MethodHandle READ_DOUBLE;
    /** Tuple method handler. */
    public static final MethodHandle READ_BYTE_BOXED;
    /** Tuple method handler. */
    public static final MethodHandle READ_SHORT_BOXED;
    /** Tuple method handler. */
    public static final MethodHandle READ_INT_BOXED;
    /** Tuple method handler. */
    public static final MethodHandle READ_LONG_BOXED;
    /** Tuple method handler. */
    public static final MethodHandle READ_FLOAT_BOXED;
    /** Tuple method handler. */
    public static final MethodHandle READ_DOUBLE_BOXED;
    /** Tuple method handler. */
    public static final MethodHandle READ_UUID;
    /** Tuple method handler. */
    public static final MethodHandle READ_BITSET;
    /** Tuple method handler. */
    public static final MethodHandle READ_STRING;
    /** Tuple method handler. */
    public static final MethodHandle READ_BYTE_ARR;

    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_NULL;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_BYTE;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_SHORT;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_INT;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_LONG;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_FLOAT;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_DOUBLE;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_UUID;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_BITSET;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_STRING;
    /** Tuple assembler method handler. */
    public static final MethodHandle WRITE_BYTE_ARR;

    /**
     * Initializes static handlers.
     */
    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            READ_BYTE = lookup.findVirtual(Tuple.class, "byteValue", MethodType.methodType(byte.class, int.class));
            READ_SHORT = lookup.findVirtual(Tuple.class, "shortValue", MethodType.methodType(short.class, int.class));
            READ_INT = lookup.findVirtual(Tuple.class, "intValue", MethodType.methodType(int.class, int.class));
            READ_LONG = lookup.findVirtual(Tuple.class, "longValue", MethodType.methodType(long.class, int.class));
            READ_FLOAT = lookup.findVirtual(Tuple.class, "floatValue", MethodType.methodType(float.class, int.class));
            READ_DOUBLE = lookup.findVirtual(Tuple.class, "doubleValue", MethodType.methodType(double.class, int.class));
            READ_BYTE_BOXED = lookup.findVirtual(Tuple.class, "byteValueBoxed", MethodType.methodType(Byte.class, int.class));
            READ_SHORT_BOXED = lookup.findVirtual(Tuple.class, "shortValueBoxed", MethodType.methodType(Short.class, int.class));
            READ_INT_BOXED = lookup.findVirtual(Tuple.class, "intValueBoxed", MethodType.methodType(Integer.class, int.class));
            READ_LONG_BOXED = lookup.findVirtual(Tuple.class, "longValueBoxed", MethodType.methodType(Long.class, int.class));
            READ_FLOAT_BOXED = lookup.findVirtual(Tuple.class, "floatValueBoxed", MethodType.methodType(Float.class, int.class));
            READ_DOUBLE_BOXED = lookup.findVirtual(Tuple.class, "doubleValueBoxed", MethodType.methodType(Double.class, int.class));
            READ_UUID = lookup.findVirtual(Tuple.class, "uuidValue", MethodType.methodType(UUID.class, int.class));
            READ_BITSET = lookup.findVirtual(Tuple.class, "bitmaskValue", MethodType.methodType(BitSet.class, int.class));
            READ_STRING = lookup.findVirtual(Tuple.class, "stringValue", MethodType.methodType(String.class, int.class));
            READ_BYTE_ARR = lookup.findVirtual(Tuple.class, "bytesValue", MethodType.methodType(byte[].class, int.class));

            WRITE_NULL = lookup.findVirtual(TupleAssembler.class, "appendNull", MethodType.methodType(void.class));
            WRITE_BYTE = lookup.findVirtual(TupleAssembler.class, "appendByte", MethodType.methodType(void.class, byte.class));
            WRITE_SHORT = lookup.findVirtual(TupleAssembler.class, "appendShort", MethodType.methodType(void.class, short.class));
            WRITE_INT = lookup.findVirtual(TupleAssembler.class, "appendInt", MethodType.methodType(void.class, int.class));
            WRITE_LONG = lookup.findVirtual(TupleAssembler.class, "appendLong", MethodType.methodType(void.class, long.class));
            WRITE_FLOAT = lookup.findVirtual(TupleAssembler.class, "appendFloat", MethodType.methodType(void.class, float.class));
            WRITE_DOUBLE = lookup.findVirtual(TupleAssembler.class, "appendDouble", MethodType.methodType(void.class, double.class));
            WRITE_UUID = lookup.findVirtual(TupleAssembler.class, "appendUuid", MethodType.methodType(void.class, UUID.class));
            WRITE_BITSET = lookup.findVirtual(TupleAssembler.class, "appendBitmask", MethodType.methodType(void.class, BitSet.class));
            WRITE_STRING = lookup.findVirtual(TupleAssembler.class, "appendString", MethodType.methodType(void.class, String.class));
            WRITE_BYTE_ARR = lookup.findVirtual(TupleAssembler.class, "appendBytes", MethodType.methodType(void.class, byte[].class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * @param mode Binary mode.
     * @param colIdx Column index in schema.
     * @return Tuple column access code generator.
     */
    static TupleColumnAccessCodeGenerator createAccessor(BinaryMode mode, int colIdx) {
        switch (mode) {
            case P_BYTE:
                return new TupleColumnAccessCodeGenerator("READ_BYTE", "WRITE_BYTE", byte.class, colIdx);
            case P_SHORT:
                return new TupleColumnAccessCodeGenerator("READ_SHORT", "WRITE_SHORT", short.class, colIdx);
            case P_INT:
                return new TupleColumnAccessCodeGenerator("READ_INT", "WRITE_INT", int.class, colIdx);
            case P_LONG:
                return new TupleColumnAccessCodeGenerator("READ_LONG", "WRITE_LONG", long.class, colIdx);
            case P_FLOAT:
                return new TupleColumnAccessCodeGenerator("READ_FLOAT", "WRITE_FLOAT", float.class, colIdx);
            case P_DOUBLE:
                return new TupleColumnAccessCodeGenerator("READ_DOUBLE", "WRITE_DOUBLE", double.class, colIdx);
            case BYTE:
                return new TupleColumnAccessCodeGenerator("READ_BYTE_BOXED", "WRITE_BYTE", Byte.class, byte.class, colIdx);
            case SHORT:
                return new TupleColumnAccessCodeGenerator("READ_SHORT_BOXED", "WRITE_SHORT", Short.class, short.class, colIdx);
            case INT:
                return new TupleColumnAccessCodeGenerator("READ_INT_BOXED", "WRITE_INT", Integer.class, int.class, colIdx);
            case LONG:
                return new TupleColumnAccessCodeGenerator("READ_LONG_BOXED", "WRITE_LONG", Long.class, long.class, colIdx);
            case FLOAT:
                return new TupleColumnAccessCodeGenerator("READ_FLOAT_BOXED", "WRITE_FLOAT", Float.class, float.class, colIdx);
            case DOUBLE:
                return new TupleColumnAccessCodeGenerator("READ_DOUBLE_BOXED", "WRITE_DOUBLE", Double.class, double.class, colIdx);
            case STRING:
                return new TupleColumnAccessCodeGenerator("READ_STRING", "WRITE_STRING", String.class, colIdx);
            case UUID:
                return new TupleColumnAccessCodeGenerator("READ_UUID", "WRITE_UUID", UUID.class, colIdx);
            case BYTE_ARR:
                return new TupleColumnAccessCodeGenerator("READ_BYTE_ARR", "WRITE_BYTE_ARR", byte[].class, colIdx);
            case BITSET:
                return new TupleColumnAccessCodeGenerator("READ_BITSET", "WRITE_BITSET", BitSet.class, colIdx);
        }

        throw new IllegalStateException("Unsupported binary mode: " + mode);
    }

    /** Reader handle name. */
    private final String readHandleName;

    /** Writer handle name. */
    private final String writeHandleName;

    /** Mapped value type. */
    private final Class<?> mappedType;

    /** Write method argument type. */
    private final Class<?> writeArgType;

    /** Column index in schema. */
    private final int colIdx;

    /**
     * Constructor.
     *
     * @param readHandleName Reader handle name.
     * @param writeHandleName Writer handle name.
     * @param mappedType Mapped value type.
     * @param colIdx Column index in schema.
     */
    TupleColumnAccessCodeGenerator(String readHandleName, String writeHandleName, Class<?> mappedType, int colIdx) {
        this(readHandleName, writeHandleName, mappedType, mappedType, colIdx);
    }

    /**
     * Constructor.
     *
     * @param readHandleName Reader handle name.
     * @param writeHandleName Writer handle name.
     * @param mappedType Mapped value type.
     * @param writeArgType Write method argument type.
     * @param colIdx Column index in schema.
     */
    TupleColumnAccessCodeGenerator(String readHandleName, String writeHandleName, Class<?> mappedType,
        Class<?> writeArgType, int colIdx) {
        this.readHandleName = readHandleName;
        this.writeHandleName = writeHandleName;
        this.colIdx = colIdx;
        this.mappedType = mappedType;
        this.writeArgType = writeArgType;
    }

    /**
     * @return Column index in schema.
     */
    public int columnIdx() {
        return colIdx;
    }

    /**
     * @param tuple Tuple.
     * @return Code that reads column value from tuple.
     */
    public CodeBlock read(String tuple) {
        return CodeBlock.of("($T)$T.$L.invokeExact($L, $L)", mappedType, TupleColumnAccessCodeGenerator.class, readHandleName, tuple, colIdx);
    }

    /**
     * @param asmVar Tuple assembler var.
     * @param valExpr Value expression.
     * @return Code that writes value to tuple column.
     */
    public CodeBlock write(String asmVar, String valExpr) {
        if (mappedType.isPrimitive())
            return CodeBlock.builder().addStatement("$T.$L.invokeExact($L, ($T)$L)", TupleColumnAccessCodeGenerator.class, writeHandleName, asmVar, writeArgType, valExpr).build();
        else {
            return CodeBlock.builder()
                .add("{\n").indent()
                .addStatement("Object fVal")
                .beginControlFlow("if((fVal = $L) == null)", valExpr)
                .addStatement("$T.WRITE_NULL.invokeExact($L)", TupleColumnAccessCodeGenerator.class, asmVar)
                .nextControlFlow("else")
                .addStatement("$T.$L.invokeExact($L, ($T)fVal)", TupleColumnAccessCodeGenerator.class, writeHandleName, asmVar, writeArgType)
                .endControlFlow()
                .unindent()
                .add("}\n")
                .build();
        }
    }
}
