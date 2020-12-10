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

import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.schema.marshaller.generator.JaninoSerializerGenerator.LF;

/**
 * Object field access expression generators.
 */
class FieldAccessExprGenerator {
    /** Append null expression. */
    private static final String WRITE_NULL_EXPR = "asm.appendNull();";

    /**
     * Created object access expressions generator.
     *
     * @param mode Field access binary mode.
     * @param colIdx Column absolute index in schema.
     * @return Object field access expressions generator.
     */
    static FieldAccessExprGenerator createIdentityAccessor(BinaryMode mode, int colIdx) {
        return createAccessor(mode, colIdx, -1L);
    }

    /**
     * Created object field access expressions generator.
     *
     * @param mode Field access binary mode.
     * @param colIdx Column absolute index in schema.
     * @param offset Object field offset.
     * @return Object field access expressions generator.
     */
    static FieldAccessExprGenerator createAccessor(BinaryMode mode, int colIdx, long offset) {
        switch (mode) {
            case BYTE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Byte",
                    "tuple.byteValueBoxed",
                    "asm.appendByte",
                    offset);

            case P_BYTE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.byteValue",
                    "asm.appendByte",
                    offset,
                    "IgniteUnsafeUtils.getByteField",
                    "IgniteUnsafeUtils.putByteField"
                );

            case SHORT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Short",
                    "tuple.shortValueBoxed",
                    "asm.appendShort",
                    offset);

            case P_SHORT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.shortValue",
                    "asm.appendShort",
                    offset,
                    "IgniteUnsafeUtils.getShortField",
                    "IgniteUnsafeUtils.putShortField"
                );

            case INT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Integer",
                    "tuple.intValueBoxed",
                    "asm.appendInt",
                    offset);

            case P_INT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.intValue",
                    "asm.appendInt",
                    offset,
                    "IgniteUnsafeUtils.getIntField",
                    "IgniteUnsafeUtils.putIntField"
                );

            case LONG:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Long",
                    "tuple.longValueBoxed",
                    "asm.appendLong",
                    offset);

            case P_LONG:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.longValue",
                    "asm.appendLong",
                    offset,
                    "IgniteUnsafeUtils.getLongField",
                    "IgniteUnsafeUtils.putLongField"
                );

            case FLOAT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Float",
                    "tuple.floatValueBoxed",
                    "asm.appendFloat",
                    offset);

            case P_FLOAT:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.floatValue",
                    "asm.appendFloat",
                    offset,
                    "IgniteUnsafeUtils.getFloatField",
                    "IgniteUnsafeUtils.putFloatField"
                );

            case DOUBLE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "Double",
                    "tuple.doubleValueBoxed",
                    "asm.appendDouble",
                    offset);

            case P_DOUBLE:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "tuple.doubleValue",
                    "asm.appendDouble",
                    offset,
                    "IgniteUnsafeUtils.getDoubleField",
                    "IgniteUnsafeUtils.putDoubleField"
                );

            case UUID:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "UUID",
                    "tuple.uuidValue", "asm.appendUuid",
                    offset);

            case BITSET:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "BitSet",
                    "tuple.bitmaskValue", "asm.appendBitmask",
                    offset);

            case STRING:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "String",
                    "tuple.stringValue", "asm.appendString",
                    offset);

            case BYTE_ARR:
                return new FieldAccessExprGenerator(
                    colIdx,
                    "byte[]",
                    "tuple.bytesValue", "asm.appendBytes",
                    offset);
            default:
                throw new IllegalStateException("Unsupportd binary mode");
        }
    }

    /** Object field offset or {@code -1} for identity accessor. */
    private final long offset;

    /** Absolute schema index. */
    private final int colIdx;

    /** Class cast expression. */
    private final String classExpr;

    /** Write column value expression. */
    private final String writeColMethod;

    /** Read column value expression. */
    private final String readColMethod;

    /** Read object field expression. */
    private final String getFieldMethod;

    /** Write object field expression. */
    private final String putFieldMethod;

    /**
     * Constructor.
     *
     * @param colIdx Absolute schema index in schema.
     * @param castClassExpr Class cast expression
     * @param readColMethod Read column value expression.
     * @param writeColMethod Write column value expression.
     * @param offset Field offset or {@code -1} for identity accessor.
     */
    private FieldAccessExprGenerator(
        int colIdx,
        String castClassExpr,
        String readColMethod,
        String writeColMethod,
        long offset
    ) {
        this(colIdx, castClassExpr, readColMethod, writeColMethod, offset,
            "IgniteUnsafeUtils.getObjectField", "IgniteUnsafeUtils.putObjectField");
    }

    /**
     * Constructor.
     *
     * @param colIdx Absolute schema index in schema.
     * @param readColMethod Read column value expression.
     * @param writeColMethod Write column value expression.
     * @param offset Field offset or {@code -1} for identity accessor.
     * @param getFieldMethod Read object field expression.
     * @param putFieldMethod Read object field expression.
     */
    public FieldAccessExprGenerator(
        int colIdx,
        String readColMethod,
        String writeColMethod,
        long offset,
        String getFieldMethod,
        String putFieldMethod
    ) {
        this(colIdx, null /* primitive type */, readColMethod, writeColMethod, offset, getFieldMethod, putFieldMethod);
    }

    /**
     * Constructor.
     *
     * @param colIdx Absolute schema index in schema.
     * @param castClassExpr Class cast expression or {@code null} if not applicable.
     * @param readColMethod Read column value expression.
     * @param writeColMethod Write column value expression.
     * @param offset Field offset or {@code -1} for identity accessor.
     * @param getFieldMethod Read object field expression.
     * @param putFieldMethod Read object field expression.
     */
    private FieldAccessExprGenerator(
        int colIdx,
        @Nullable String castClassExpr,
        String readColMethod,
        String writeColMethod,
        long offset,
        String getFieldMethod,
        String putFieldMethod
    ) {
        this.offset = offset;
        this.colIdx = colIdx;
        this.classExpr = castClassExpr;
        this.putFieldMethod = putFieldMethod;
        this.getFieldMethod = getFieldMethod;
        this.writeColMethod = writeColMethod;
        this.readColMethod = readColMethod;
    }

    /**
     * @return {@code true} if it is primitive typed field accessor, {@code false} otherwise.
     */
    private boolean isPrimitive() {
        return classExpr == null;
    }

    /**
     * @return {@code true} if is identity accessor, {@code false} otherwise.
     */
    private boolean isIdentityAccessor() {
        return offset == -1;
    }

    /**
     * @return Object field value access expression or object value expression for simple types.
     */
    public String getFieldExpr() {
        if (isIdentityAccessor())
            return "obj"; // Identity accessor.

        return getFieldMethod + "(obj, " + offset + ')';
    }

    /**
     * Appends write value to field expression.
     *
     * @param sb String bulder.
     * @param valueExpression Value expression.
     * @param indent Line indentation.
     */
    public final void appendPutFieldExpr(StringBuilder sb, String valueExpression, String indent) {
        sb.append(indent).append(putFieldMethod).append("(obj, ").append(offset).append(", ").append(valueExpression).append(')');
        sb.append(";" + LF);
    }

    /**
     * Appends write value to column expression.
     *
     * @param sb String bulder.
     * @param valueExpr Value expression.
     * @param indent Line indentation.
     */
    public final void appendWriteColumnExpr(StringBuilder sb, String valueExpr, String indent) {
        if (isPrimitive() || isIdentityAccessor()) {
            // Translate to:
            // asm.appendX((T) %value%);
            // or for primitive value:
            // asm.appendX(%value%);
            sb.append(indent).append(writeColMethod).append('(');

            if (classExpr != null)
                sb.append("(").append(classExpr).append(")");

            sb.append(valueExpr).append(");" + LF);

            return;
        }

        assert classExpr != null;

        // Translate to:
        // { T fVal = (T)%value%;
        //  if (fVal == null) asm.appendNull() else asm.appendX(fVal); }
        sb.append(indent).append("{ ").append(classExpr).append(" fVal = (").append(classExpr).append(')').append(valueExpr).append(";" + LF);
        sb.append(indent).append("if (fVal == null) " + WRITE_NULL_EXPR + LF);
        sb.append(indent).append("else ").append(writeColMethod).append("(fVal); }" + LF);

    }

    /**
     * @return Column value read expression.
     */
    public String readColumnExpr() {
        return readColMethod + "(" + colIdx + ")";
    }
}
