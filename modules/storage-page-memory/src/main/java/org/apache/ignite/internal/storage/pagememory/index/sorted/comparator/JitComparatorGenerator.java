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

package org.apache.ignite.internal.storage.pagememory.index.sorted.comparator;

import static com.facebook.presto.bytecode.control.SwitchStatement.switchBuilder;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.bitwiseAnd;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.bitwiseOr;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.notEqual;
import static com.facebook.presto.bytecode.instruction.JumpInstruction.jump;
import static org.apache.ignite.internal.binarytuple.BinaryTupleParser.shortValue;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.control.SwitchStatement.SwitchBuilder;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoLocalDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTupleFormatException;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.ByteBufferAccessor;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.schema.BinaryTupleComparatorUtils;
import org.apache.ignite.internal.schema.UnsafeByteBufferAccessor;
import org.apache.ignite.internal.storage.pagememory.index.sorted.io.SortedIndexTreeIo;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * Generator for implementation of {@link JitComparator} using bytecode generation.
 */
public class JitComparatorGenerator {
    /**
     * Class generator. Please use {@link ClassGenerator#dumpClassFilesTo(Path)} for debugging or investigations.
     */
    private static final ClassGenerator CLASS_GENERATOR = ClassGenerator.classGenerator(SortedIndexTreeIo.class.getClassLoader());

    // Methods of BinaryTupleParser.
    private static final Method PARSER_BYTE_VALUE;
    private static final Method PARSER_SHORT_VALUE;
    private static final Method PARSER_INT_VALUE;
    private static final Method PARSER_LONG_VALUE;
    private static final Method PARSER_FLOAT_VALUE;
    private static final Method PARSER_DOUBLE_VALUE;
    private static final Method PARSER_DATE_VALUE;
    private static final Method PARSER_TIME_VALUE;
    private static final Method PARSER_DATETIME_VALUE;

    // "compare" methods of primitive types.
    private static final Method BYTE_COMPARE;
    private static final Method SHORT_COMPARE;
    private static final Method INT_COMPARE;
    private static final Method LONG_COMPARE;
    private static final Method FLOAT_COMPARE;
    private static final Method DOUBLE_COMPARE;

    // Methods of BinaryTupleComparatorUtils.
    private static final Method UTILS_TIMESTAMP_COMPARE;
    private static final Method UTILS_UUID_COMPARE;
    private static final Method UTILS_STRING_COMPARE;
    private static final Method UTILS_BYTES_COMPARE;

    // "compareBigDecimal" from this class.
    private static final Method DECIMAL_COMPARE;

    // "compareTo" methods of LocalDate, LocalTime, and LocalDateTime.
    private static final Method DATE_COMPARE_TO;
    private static final Method TIME_COMPARE_TO;
    private static final Method DATETIME_COMPARE_TO;

    static {
        try {
            PARSER_BYTE_VALUE = BinaryTupleParser.class.getDeclaredMethod("byteValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_SHORT_VALUE = BinaryTupleParser.class.getDeclaredMethod("shortValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_INT_VALUE = BinaryTupleParser.class.getDeclaredMethod("intValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_LONG_VALUE = BinaryTupleParser.class.getDeclaredMethod("longValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_FLOAT_VALUE = BinaryTupleParser.class.getDeclaredMethod("floatValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_DOUBLE_VALUE = BinaryTupleParser.class.getDeclaredMethod("doubleValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_DATE_VALUE = BinaryTupleParser.class.getDeclaredMethod("dateValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_TIME_VALUE = BinaryTupleParser.class.getDeclaredMethod("timeValue", ByteBufferAccessor.class, int.class, int.class);
            PARSER_DATETIME_VALUE = BinaryTupleParser.class.getDeclaredMethod(
                    "dateTimeValue", ByteBufferAccessor.class, int.class, int.class
            );

            BYTE_COMPARE = Byte.class.getDeclaredMethod("compare", byte.class, byte.class);
            SHORT_COMPARE = Short.class.getDeclaredMethod("compare", short.class, short.class);
            INT_COMPARE = Integer.class.getDeclaredMethod("compare", int.class, int.class);
            LONG_COMPARE = Long.class.getDeclaredMethod("compare", long.class, long.class);
            FLOAT_COMPARE = Float.class.getDeclaredMethod("compare", float.class, float.class);
            DOUBLE_COMPARE = Double.class.getDeclaredMethod("compare", double.class, double.class);

            UTILS_TIMESTAMP_COMPARE = BinaryTupleComparatorUtils.class.getDeclaredMethod(
                    "compareAsTimestamp", ByteBufferAccessor.class, int.class, int.class, ByteBufferAccessor.class, int.class, int.class
            );
            UTILS_UUID_COMPARE = BinaryTupleComparatorUtils.class.getDeclaredMethod(
                    "compareAsUuid", ByteBufferAccessor.class, int.class, ByteBufferAccessor.class, int.class
            );
            UTILS_STRING_COMPARE = BinaryTupleComparatorUtils.class.getDeclaredMethod(
                    "compareAsString", ByteBufferAccessor.class, int.class, int.class, ByteBufferAccessor.class, int.class, int.class
            );
            UTILS_BYTES_COMPARE = BinaryTupleComparatorUtils.class.getDeclaredMethod(
                    "compareAsBytes", ByteBufferAccessor.class, int.class, int.class, ByteBufferAccessor.class, int.class, int.class
            );

            DECIMAL_COMPARE = JitComparatorGenerator.class.getDeclaredMethod(
                    "compareBigDecimal",
                    UnsafeByteBufferAccessor.class, int.class, int.class, UnsafeByteBufferAccessor.class, int.class, int.class
            );

            DATE_COMPARE_TO = LocalDate.class.getDeclaredMethod("compareTo", ChronoLocalDate.class);
            TIME_COMPARE_TO = LocalTime.class.getDeclaredMethod("compareTo", LocalTime.class);
            DATETIME_COMPARE_TO = LocalDateTime.class.getDeclaredMethod("compareTo", ChronoLocalDateTime.class);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Counter for generating class names. */
    private static final AtomicInteger CLASS_NAME_COUNTER = new AtomicInteger();

    /**
     * Creates an instance of {@link JitComparator} using bytecode generation.
     *
     * @param columnCollations List of column collations.
     * @param columnTypes List of column types.
     * @param nullableFlags List of nullability flags for each column.
     */
    public static JitComparator createComparator(
            List<CatalogColumnCollation> columnCollations,
            List<NativeType> columnTypes,
            List<Boolean> nullableFlags
    ) {
        int maxEntrySizeLog = maxEntrySizeLog(columnTypes);

        // Name "org/apache/ignite/internal/storage/pagememory/index/sorted/comparator/JitComparatorGenerator&&<idx>" is used for generated
        // classes.
        ClassDefinition classDefinition = new ClassDefinition(
                EnumSet.of(Access.PUBLIC, Access.FINAL),
                JitComparator.class.getName().replace('.', '/') + "$$" + CLASS_NAME_COUNTER.getAndIncrement(),
                ParameterizedType.type(Object.class),
                ParameterizedType.type(JitComparator.class)
        );

        // The implementation of the "JitComparator#compare" method.
        MethodDefinition compare = classDefinition.declareMethod(
                EnumSet.of(Access.PUBLIC, Access.FINAL),
                "compare",
                ParameterizedType.type(int.class),
                Parameter.arg("outerAccessor", UnsafeByteBufferAccessor.class),
                Parameter.arg("outerSize", int.class),
                Parameter.arg("innerAccessor", UnsafeByteBufferAccessor.class),
                Parameter.arg("innerSize", int.class)
        );
        compare.declareAnnotation(Override.class);

        Scope scope = compare.getScope();
        BytecodeBlock body = compare.getBody();

        // Method parameters.
        Variable outerAccessor = scope.getVariable("outerAccessor");
        Variable innerAccessor = scope.getVariable("innerAccessor");
        Variable outerSize = scope.getVariable("outerSize");
        Variable innerSize = scope.getVariable("innerSize");

        // Variables for entry sizes. If the "maxEntrySizeLog" is "0", then these variables will left unused, because we know their value at
        // compile time (it's always zero).
        Variable outerEntrySize = scope.declareVariable(int.class, "outerEntrySize");
        Variable innerEntrySize = scope.declareVariable(int.class, "innerEntrySize");

        if (maxEntrySizeLog != 0) {
            // If entry size can be more than 1, then we read it from headers of tuples like this:
            //  int outerFlag = outerAccessor.get(0) & BinaryTupleCommon.VARSIZE_MASK;
            //  int outerEntrySize = 1 << outerFlag;
            Variable outerFlag = scope.declareVariable(byte.class, "outerFlag");
            Variable innerFlag = scope.declareVariable(byte.class, "innerFlag");

            body.append(outerFlag.set(outerAccessor.invoke("get", byte.class, constantInt(0))));
            body.append(innerFlag.set(innerAccessor.invoke("get", byte.class, constantInt(0))));

            body.append(outerEntrySize.set(bitwiseAnd(outerFlag.cast(int.class), constantInt(BinaryTupleCommon.VARSIZE_MASK))));
            body.append(innerEntrySize.set(bitwiseAnd(innerFlag.cast(int.class), constantInt(BinaryTupleCommon.VARSIZE_MASK))));
        }

        // Here we generate all possible combinations of comparators for all possible entry sizes. These methods will look like this
        //  int innerCompareXY(UnsafeByteBufferAccessor outerAccessor, int outerSize, UnsafeByteBufferAccessor innerAccessor, int innerSize)
        // where X and Y are entry sizes in bytes (1, 2, or 4), for "outer" and "inner" tuples respectively.
        MethodDefinition[][] innerCompareMethods = new MethodDefinition[3][3];
        for (int i = 0; i <= maxEntrySizeLog; i++) {
            for (int j = 0; j <= maxEntrySizeLog; j++) {
                innerCompareMethods[i][j] = innerCompare(classDefinition, columnCollations, columnTypes, nullableFlags, 1 << i, 1 << j);
            }
        }

        // "compare" method implementation will either look like this:
        //  return innerCompare11(outerAccessor, outerSize, innerAccessor, innerSize);
        // if "maxEntrySize" is predetermined, or like the comment in "else" branch.
        if (maxEntrySizeLog == 0) {
            body.append(invokeStatic(innerCompareMethods[0][0], outerAccessor, outerSize, innerAccessor, innerSize).ret());
        } else {
            // Alternative representation will look like this (entrySize is either 0, 1 or 2):
            //  switch (outerEntrySize) {
            //      case 0:
            //          switch (innerEntrySize) {
            //              case 0:
            //                  return innerCompare11(outerAccessor, outerSize, innerAccessor, innerSize);
            //              case 1:
            //                  return innerCompare12(outerAccessor, outerSize, innerAccessor, innerSize);
            //              case 2:
            //                  return innerCompare13(outerAccessor, outerSize, innerAccessor, innerSize);
            //              default:
            //                  return 0;
            //          }
            //      case 1:
            //          switch (innerEntrySize) {
            //  ...
            //  }
            // "case 2" can be missing if "maxEntrySizeLog" is equal to "1".
            SwitchBuilder outerSwitchBuilder = switchBuilder()
                    .expression(outerEntrySize)
                    .defaultCase(constantInt(0).ret());

            for (int i = 0; i <= maxEntrySizeLog; i++) {
                SwitchBuilder innerSwitchBuilder = switchBuilder()
                        .expression(innerEntrySize)
                        .defaultCase(constantInt(0).ret());

                for (int j = 0; j <= maxEntrySizeLog; j++) {
                    MethodDefinition innerCompare = innerCompareMethods[i][j];

                    innerSwitchBuilder.addCase(j, invokeStatic(innerCompare, outerAccessor, outerSize, innerAccessor, innerSize).ret());
                }

                outerSwitchBuilder.addCase(i, innerSwitchBuilder.build());
            }

            body.append(outerSwitchBuilder.build());
        }

        // Final "return 0" statement.
        body.append(constantInt(0).ret());

        Class<?> clazz = CLASS_GENERATOR.defineClass(classDefinition, Object.class);
        try {
            //noinspection ClassNewInstance,deprecation
            return (JitComparator) clazz.newInstance();
        } catch (Exception e) {
            throw new IgniteInternalException(Common.INTERNAL_ERR, e);
        }
    }

    /**
     * Determines how large the offset value has to be in the worst case for the given column types.
     * Returns {@code 0} if {@code 1} byte is enough.
     * Returns {@code 1} if {@code 2} bytes are enough.
     * Returns {@code 2} otherwise.
     *
     * @param columnTypes List of column types.
     */
    private static int maxEntrySizeLog(List<NativeType> columnTypes) {
        if (columnTypes.stream().allMatch(NativeType::fixedLength)) {
            int maxFixedSize = columnTypes.stream().mapToInt(NativeType::sizeInBytes).sum();
            if (maxFixedSize < 0x100) {
                return 0;
            } else if (maxFixedSize < 0x10000) {
                return 1;
            } else {
                return 2;
            }
        } else {
            return 2;
        }
    }

    /**
     * Returns a comparator method, for which both entry sizes are known at compile time.
     */
    private static MethodDefinition innerCompare(
            ClassDefinition classDefinition,
            List<CatalogColumnCollation> columnCollations,
            List<NativeType> columnTypes,
            List<Boolean> nullableFlags,
            int outerEntrySizeConstant,
            int innerEntrySizeConstant
    ) {
        MethodDefinition innerCompare = classDefinition.declareMethod(
                EnumSet.of(Access.PRIVATE, Access.STATIC),
                "innerCompare" + outerEntrySizeConstant + innerEntrySizeConstant,
                ParameterizedType.type(int.class),
                Parameter.arg("outerAccessor", UnsafeByteBufferAccessor.class),
                Parameter.arg("outerSize", int.class),
                Parameter.arg("innerAccessor", UnsafeByteBufferAccessor.class),
                Parameter.arg("innerSize", int.class)
        );

        BytecodeBlock body = innerCompare.getBody();
        Scope scope = innerCompare.getScope();

        Variable outerAccessor = scope.getVariable("outerAccessor");
        Variable innerAccessor = scope.getVariable("innerAccessor");
        Variable outerSize = scope.getVariable("outerSize");
        Variable innerSize = scope.getVariable("innerSize");

        Variable outerEntryBaseStart = scope.declareVariable(int.class, "outerEntryBaseStart");
        Variable innerEntryBaseStart = scope.declareVariable(int.class, "innerEntryBaseStart");

        // Here we do exactly the same thing that "BinaryTupleComparator.compare" does, but types and offsets are inlined, and the loop is
        // unrolled. Please use that method as a reference for understanding this code.
        int columnsSize = columnTypes.size();
        body.append(outerEntryBaseStart.set(constantInt(BinaryTupleCommon.HEADER_SIZE + outerEntrySizeConstant * columnsSize)));
        body.append(innerEntryBaseStart.set(constantInt(BinaryTupleCommon.HEADER_SIZE + innerEntrySizeConstant * columnsSize)));

        Variable cmp = scope.declareVariable(int.class, "cmp");

        Variable outerEntryBaseEnd = scope.declareVariable(int.class, "outerEntryBaseEnd");
        Variable innerEntryBaseEnd = scope.declareVariable(int.class, "innerEntryBaseEnd");
        Variable outerInNull = scope.declareVariable(boolean.class, "outerInNull");
        Variable innerInNull = scope.declareVariable(boolean.class, "innerInNull");
        for (int i = 0; i < columnsSize; i++) {
            // Last iteration doesn't need to use "cmp" variable, we can return comparison results directly.
            boolean lastIteration = i == columnsSize - 1;
            LabelNode endOfBlockLabel = new LabelNode();

            if (lastIteration) {
                body.append(outerEntryBaseEnd.set(outerSize));
                body.append(innerEntryBaseEnd.set(innerSize));
            } else {
                body.append(outerEntryBaseEnd.set(add(
                        constantInt(BinaryTupleCommon.HEADER_SIZE + outerEntrySizeConstant * columnsSize),
                        getOffset(outerAccessor, outerEntrySizeConstant, i)
                )));
                body.append(innerEntryBaseEnd.set(add(
                        constantInt(BinaryTupleCommon.HEADER_SIZE + innerEntrySizeConstant * columnsSize),
                        getOffset(innerAccessor, innerEntrySizeConstant, i)
                )));
            }

            CatalogColumnCollation collation = columnCollations.get(i);
            NativeType columnType = columnTypes.get(i);

            // Nullability check.
            if (nullableFlags.get(i)) {
                body.append(outerInNull.set(equal(outerEntryBaseStart, outerEntryBaseEnd)));
                body.append(innerInNull.set(equal(innerEntryBaseStart, innerEntryBaseEnd)));

                body.append(new IfStatement()
                        .condition(bitwiseAnd(outerInNull, innerInNull))
                        .ifTrue(lastIteration ? constantInt(0).ret() : jump(endOfBlockLabel))
                        .ifFalse(new IfStatement()
                                .condition(bitwiseOr(outerInNull, innerInNull))
                                .ifTrue(new IfStatement()
                                        .condition(outerInNull)
                                        .ifTrue((collation.nullsFirst() ? constantInt(-1) : constantInt(1)).ret())
                                        .ifFalse((collation.nullsFirst() ? constantInt(1) : constantInt(-1)).ret())
                                )
                        )
                );
            }

            BytecodeExpression compareExpression = compareTupleElement(
                    collation, columnType,
                    new ComparisonVariables(
                            outerAccessor, outerEntryBaseStart, outerEntryBaseEnd,
                            innerAccessor, innerEntryBaseStart, innerEntryBaseEnd
                    )
            );

            if (lastIteration) {
                body.append(compareExpression.ret());
            } else {
                body.append(cmp.set(compareExpression));

                body.append(new IfStatement()
                        .condition(notEqual(constantInt(0), cmp))
                        .ifTrue(cmp.ret())
                );

                body.append(endOfBlockLabel);

                body.append(outerEntryBaseStart.set(outerEntryBaseEnd));
                body.append(innerEntryBaseStart.set(innerEntryBaseEnd));
            }
        }

        return innerCompare;
    }

    /**
     * Generates an expression that reads the offset from the given position. Mirrors the code from "BinaryTupleParser.OffsetTableReader".
     */
    private static BytecodeExpression getOffset(Variable accessor, int entrySizeConstant, int position) {
        switch (entrySizeConstant) {
            case 1:
                return invokeStatic(Byte.class, "toUnsignedInt", int.class,
                        accessor.invoke("get", byte.class, constantInt(BinaryTupleCommon.HEADER_SIZE + position))
                );
            case 2:
                return invokeStatic(Short.class, "toUnsignedInt", int.class,
                        accessor.invoke("getShort", short.class, constantInt(BinaryTupleCommon.HEADER_SIZE + 2 * position))
                );
            case 4:
                return accessor.invoke("getInt", int.class, constantInt(BinaryTupleCommon.HEADER_SIZE + 4 * position));
            default:
                throw new IllegalArgumentException("Unsupported entry size constant: " + entrySizeConstant);
        }
    }

    /**
     * Just a small helper class to reduce the number of arguments in private methods here.
     */
    private static class ComparisonVariables {
        final Variable outerAccessor;
        final Variable outerEntryBaseStart;
        final Variable outerEntryBaseEnd;
        final Variable innerAccessor;
        final Variable innerEntryBaseStart;
        final Variable innerEntryBaseEnd;

        ComparisonVariables(
                Variable outerAccessor,
                Variable outerEntryBaseStart,
                Variable outerEntryBaseEnd,
                Variable innerAccessor,
                Variable innerEntryBaseStart,
                Variable innerEntryBaseEnd
        ) {
            this.outerAccessor = outerAccessor;
            this.outerEntryBaseStart = outerEntryBaseStart;
            this.outerEntryBaseEnd = outerEntryBaseEnd;
            this.innerAccessor = innerAccessor;
            this.innerEntryBaseStart = innerEntryBaseStart;
            this.innerEntryBaseEnd = innerEntryBaseEnd;
        }
    }

    /**
     * Generates an expression that compares a specific element of two binary tuples.
     */
    private static BytecodeExpression compareTupleElement(
            CatalogColumnCollation collation,
            NativeType nativeType,
            ComparisonVariables vars
    ) {
        switch (nativeType.spec()) {
            case BOOLEAN:
            case INT8:
                return compositeStaticCompare(collation, vars, PARSER_BYTE_VALUE, BYTE_COMPARE);
            case INT16:
                return compositeStaticCompare(collation, vars, PARSER_SHORT_VALUE, SHORT_COMPARE);
            case INT32:
                return compositeStaticCompare(collation, vars, PARSER_INT_VALUE, INT_COMPARE);
            case INT64:
                return compositeStaticCompare(collation, vars, PARSER_LONG_VALUE, LONG_COMPARE);
            case FLOAT:
                return compositeStaticCompare(collation, vars, PARSER_FLOAT_VALUE, FLOAT_COMPARE);
            case DOUBLE:
                return compositeStaticCompare(collation, vars, PARSER_DOUBLE_VALUE, DOUBLE_COMPARE);
            case DECIMAL:
                return staticCompare(collation, vars, DECIMAL_COMPARE, true);
            case DATE:
                return compositeVirtualCompare(collation, vars, PARSER_DATE_VALUE, DATE_COMPARE_TO);
            case TIME:
                return compositeVirtualCompare(collation, vars, PARSER_TIME_VALUE, TIME_COMPARE_TO);
            case DATETIME:
                return compositeVirtualCompare(collation, vars, PARSER_DATETIME_VALUE, DATETIME_COMPARE_TO);
            case TIMESTAMP:
                return staticCompare(collation, vars, UTILS_TIMESTAMP_COMPARE, true);
            case UUID:
                return staticCompare(collation, vars, UTILS_UUID_COMPARE, false);
            case STRING:
                return staticCompare(collation, vars, UTILS_STRING_COMPARE, true);
            case BYTE_ARRAY:
                return staticCompare(collation, vars, UTILS_BYTES_COMPARE, true);
            default:
                throw new IllegalArgumentException(format("Unsupported column type in binary tuple comparator. [type={}]", nativeType));
        }
    }

    /**
     * Generates an expression for comparison that looks like this: {@code comparator(extractor(value 1), extractor(value 2))}, where values
     * will be chosen depending on column's collation.
     */
    private static BytecodeExpression compositeStaticCompare(
            CatalogColumnCollation collation,
            ComparisonVariables vars,
            Method extractor,
            Method comparator
    ) {
        BytecodeExpression outerValue = invokeStatic(extractor, vars.outerAccessor, vars.outerEntryBaseStart, vars.outerEntryBaseEnd);
        BytecodeExpression innerValue = invokeStatic(extractor, vars.innerAccessor, vars.innerEntryBaseStart, vars.innerEntryBaseEnd);

        if (collation.asc()) {
            return invokeStatic(comparator, outerValue, innerValue);
        } else {
            return invokeStatic(comparator, innerValue, outerValue);
        }
    }

    /**
     * Generates an expression for comparison that looks like this: {@code extractor(value 1).comparator(extractor(value 2))}, where values
     * will be chosen depending on column's collation.
     */
    private static BytecodeExpression compositeVirtualCompare(
            CatalogColumnCollation collation,
            ComparisonVariables vars,
            Method extractor,
            Method comparator
    ) {
        BytecodeExpression outerValue = invokeStatic(extractor, vars.outerAccessor, vars.outerEntryBaseStart, vars.outerEntryBaseEnd);
        BytecodeExpression innerValue = invokeStatic(extractor, vars.innerAccessor, vars.innerEntryBaseStart, vars.innerEntryBaseEnd);

        if (collation.asc()) {
            return outerValue.invoke(comparator, innerValue);
        } else {

            return innerValue.invoke(comparator, outerValue);
        }
    }

    /**
     * Generates an expression for comparison that looks like this: {@code comparator(value 1, value 2)}, where values
     * will be chosen depending on column's collation. {@code passEnd} flag reflects the number of arguments in {@code comparator}.
     */
    private static BytecodeExpression staticCompare(
            CatalogColumnCollation collation,
            ComparisonVariables vars,
            Method comparator,
            boolean passEnd
    ) {
        if (collation.asc()) {
            if (passEnd) {
                return invokeStatic(
                        comparator,
                        vars.outerAccessor, vars.outerEntryBaseStart, vars.outerEntryBaseEnd,
                        vars.innerAccessor, vars.innerEntryBaseStart, vars.innerEntryBaseEnd
                );
            } else {
                return invokeStatic(
                        comparator,
                        vars.outerAccessor, vars.outerEntryBaseStart,
                        vars.innerAccessor, vars.innerEntryBaseStart
                );
            }
        } else {
            if (passEnd) {
                return invokeStatic(
                        comparator,
                        vars.innerAccessor, vars.innerEntryBaseStart, vars.innerEntryBaseEnd,
                        vars.outerAccessor, vars.outerEntryBaseStart, vars.outerEntryBaseEnd
                );
            } else {
                return invokeStatic(
                        comparator,
                        vars.innerAccessor, vars.innerEntryBaseStart,
                        vars.outerAccessor, vars.outerEntryBaseStart
                );
            }
        }
    }

    /**
     * Compares two decimal values.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-26022 Remove this method.
    public static int compareBigDecimal(
            UnsafeByteBufferAccessor buf1,
            int begin1,
            int end1,
            UnsafeByteBufferAccessor buf2,
            int begin2,
            int end2
    ) {
        BigDecimal left = decimalValue(buf1, begin1, end1);
        BigDecimal right = decimalValue(buf2, begin2, end2);

        return left.compareTo(right);
    }

    /**
     * Reads a decimal value from the tuple.
     *
     * @see BinaryTupleReader#decimalValue(int, int)
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-26022 Remove this method.
    private static BigDecimal decimalValue(UnsafeByteBufferAccessor buf, int begin, int end) {
        short scale = shortValue(buf, begin, begin + 2);

        return new BigDecimal(numberValue(buf, begin + 2, end), scale);
    }

    /**
     * Reads a number value from the tuple.
     *
     * @see BinaryTupleParser#numberValue(int, int)
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-26022 Remove this method.
    private static BigInteger numberValue(UnsafeByteBufferAccessor buf, int begin, int end) {
        int len = end - begin;
        if (len <= 0) {
            throw new BinaryTupleFormatException("Invalid length for a tuple element: " + len);
        }

        byte[] array = buf.getArray();
        byte[] bytes;
        if (array != null) {
            bytes = array;
            //noinspection NumericCastThatLosesPrecision
            begin += (int) (buf.getAddress() - GridUnsafe.BYTE_ARR_OFF);
        } else {
            bytes = GridUnsafe.getBytes(buf.getAddress(), begin, len);
        }
        return new BigInteger(bytes, begin, len);
    }
}
