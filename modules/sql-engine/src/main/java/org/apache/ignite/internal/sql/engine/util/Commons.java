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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.calcite.rel.hint.HintPredicates.AGGREGATE;
import static org.apache.calcite.rel.hint.HintPredicates.JOIN;
import static org.apache.ignite.internal.sql.engine.QueryProperty.ALLOWED_QUERY_TYPES;
import static org.apache.ignite.internal.sql.engine.util.BaseQueryContext.CLUSTER;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.sql.ColumnMetadata.UNDEFINED_PRECISION;
import static org.apache.ignite.sql.ColumnMetadata.UNDEFINED_SCALE;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.DataContexts;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.RexExecutorImpl;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.metadata.IgniteMetadata;
import org.apache.ignite.internal.sql.engine.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.prepare.IgniteConvertletTable;
import org.apache.ignite.internal.sql.engine.prepare.IgniteTypeCoercion;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCommitTransaction;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlConformance;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlStartTransaction;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.trait.DistributionTraitDef;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.type.BitmaskNativeType;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NumberNativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods.
 */
public final class Commons {
    public static final String IMPLICIT_PK_COL_NAME = "__p_key";

    public static final int IN_BUFFER_SIZE = 512;

    public static final int IO_BATCH_SIZE = 256;
    public static final int IO_BATCH_COUNT = 4;

    /**
     * The number of elements to be prefetched from each partition when scanning the sorted index.
     * The higher the value, the fewer calls to the upstream will be, but at the same time, the bigger
     * internal buffer will be.
     */
    public static final int SORTED_IDX_PART_PREFETCH_SIZE = 100;

    @SuppressWarnings("rawtypes")
    public static final List<RelTraitDef> DISTRIBUTED_TRAITS_SET = List.of(
            ConventionTraitDef.INSTANCE,
            DistributionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE
    );

    public static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
            .executor(new RexExecutorImpl(DataContexts.EMPTY))
            .sqlToRelConverterConfig(SqlToRelConverter.config()
                    .withTrimUnusedFields(true)
                    // currently SqlToRelConverter creates not optimal plan for both optimization and execution
                    // so it's better to disable such rewriting right now
                    // TODO: remove this after IGNITE-14277
                    .withInSubQueryThreshold(Integer.MAX_VALUE)
                    .withDecorrelationEnabled(true)
                    .withExpand(false)
                    .withHintStrategyTable(
                            HintStrategyTable.builder()
                                    .hintStrategy(IgniteHint.ENFORCE_JOIN_ORDER.name(), JOIN)
                                    .hintStrategy(IgniteHint.DISABLE_RULE.name(), (hint, rel) -> true)
                                    .hintStrategy(IgniteHint.EXPAND_DISTINCT_AGG.name(), AGGREGATE)
                                    .hintStrategy(IgniteHint.NO_INDEX.name(), (hint, rel) -> rel instanceof IgniteLogicalTableScan)
                                    .hintStrategy(IgniteHint.FORCE_INDEX.name(), (hint, rel) -> rel instanceof IgniteLogicalTableScan)
                                    .build()
                    )
            )
            .convertletTable(IgniteConvertletTable.INSTANCE)
            .parserConfig(IgniteSqlParser.PARSER_CONFIG)
            .sqlValidatorConfig(SqlValidator.Config.DEFAULT
                    .withIdentifierExpansion(true)
                    .withDefaultNullCollation(NullCollation.HIGH)
                    .withSqlConformance(IgniteSqlConformance.INSTANCE)
                    .withTypeCoercionRules(standardCompatibleCoercionRules())
                    .withTypeCoercionFactory(IgniteTypeCoercion::new))
            // Dialects support.
            .operatorTable(IgniteSqlOperatorTable.INSTANCE)
            // Context provides a way to store data within the planner session that can be accessed in planner rules.
            .context(Contexts.empty())
            // Custom cost factory to use during optimization
            .costFactory(new IgniteCostFactory())
            .typeSystem(IgniteTypeSystem.INSTANCE)
            .traitDefs(DISTRIBUTED_TRAITS_SET)
            .build();

    private Commons() {
    }

    private static SqlTypeCoercionRule standardCompatibleCoercionRules() {
        return SqlTypeCoercionRule.instance(IgniteCustomAssignmentsRules.instance().getTypeMapping());
    }

    /**
     * Gets appropriate field from two rows by offset.
     *
     * @param hnd RowHandler impl.
     * @param offset Current offset.
     * @param row1 row1.
     * @param row2 row2.
     * @return Returns field by offset.
     */
    public static @Nullable <RowT> Object getFieldFromBiRows(RowHandler<RowT> hnd, int offset, RowT row1, RowT row2) {
        return offset < hnd.columnCount(row1) ? hnd.get(offset, row1) :
            hnd.get(offset - hnd.columnCount(row1), row2);
    }

    /**
     * Returns a given list as a typed list.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> List<T> cast(List<?> src) {
        return (List) src;
    }

    /**
     * Transforms a given list using map function.
     */
    public static <T, R> List<R> transform(List<T> src, Function<T, R> mapFun) {
        if (nullOrEmpty(src)) {
            return Collections.emptyList();
        }

        List<R> list = new ArrayList<>(src.size());

        for (T t : src) {
            list.add(mapFun.apply(t));
        }

        return list;
    }

    /**
     * Extracts type factory.
     */
    public static IgniteTypeFactory typeFactory(RelNode rel) {
        return typeFactory(rel.getCluster());
    }

    /**
     * Extracts type factory.
     */
    public static IgniteTypeFactory typeFactory(RelOptCluster cluster) {
        return (IgniteTypeFactory) cluster.getTypeFactory();
    }

    /**
     * Standalone type factory.
     */
    public static IgniteTypeFactory typeFactory() {
        return typeFactory(emptyCluster());
    }

    /** Row-expression builder. **/
    public static RexBuilder rexBuilder() {
        return emptyCluster().getRexBuilder();
    }

    /**
     * Extracts query context.
     */
    public static BaseQueryContext context(RelNode rel) {
        return context(rel.getCluster());
    }

    /**
     * Extracts query context.
     */
    public static BaseQueryContext context(RelOptCluster cluster) {
        return Objects.requireNonNull(cluster.getPlanner().getContext().unwrap(BaseQueryContext.class));
    }

    /**
     * Extracts planner context.
     */
    public static PlanningContext context(Context ctx) {
        return Objects.requireNonNull(ctx.unwrap(PlanningContext.class));
    }

    /**
     * Creates a map of parameters values from the given array of dynamic parameter values.
     * All dynamic parameter should have their values specified.
     *
     * @param params Parameters.
     * @return Parameters map.
     */
    public static Map<String, Object> parametersMap(@Nullable Object[] params) {
        if (ArrayUtils.nullOrEmpty(params)) {
            return Collections.emptyMap();
        } else {
            HashMap<String, Object> res = new HashMap<>();

            populateParameters(res, params);

            return res;
        }
    }

    /**
     * Creates an array from the given map in which array indices become map keys. e.g: [1, null, "3"] -> {0: 1, 1: null, 2: "3"}.
     * If the given array is null, this method returns an empty map.
     *
     * @param params Array of values.
     * @return Map of values.
     */
    public static Int2ObjectMap<Object> arrayToMap(@Nullable Object[] params) {
        if (ArrayUtils.nullOrEmpty(params)) {
            return Int2ObjectMaps.emptyMap();
        } else {
            Int2ObjectMap<Object> res = new Int2ObjectArrayMap<>(params.length);

            for (int i = 0; i < params.length; i++) {
                res.put(i, params[i]);
            }

            return res;
        }
    }

    /**
     * Populates a provided map with given parameters.
     *
     * @param dst    Map to populate.
     * @param params Parameters.
     */
    private static void populateParameters(Map<String, Object> dst, Object[] params) {
        for (int i = 0; i < params.length; i++) {
            dst.put("?" + i, params[i]);
        }
    }

    /**
     * Flat.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T> List<T> flat(List<List<? extends T>> src) {
        return src.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    /**
     * Compile.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T> T compile(Class<T> interfaceType, String body) {
        final boolean debug = CalciteSystemProperty.DEBUG.value();

        if (debug) {
            Util.debugCode(System.out, body);
        }

        try {
            final ICompilerFactory compilerFactory;

            try {
                compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory(ExpressionFactoryImpl.class.getClassLoader());
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Unable to instantiate java compiler", e);
            }

            IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();

            cbe.setImplementedInterfaces(new Class[]{interfaceType});

            if (debug) {
                // Add line numbers to the generated janino class
                cbe.setDebuggingInformation(true, true, true);
            }

            return (T) cbe.createInstance(new StringReader(body));
        } catch (Exception e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Unable to compile expression", e);
        }
    }

    /**
     * CheckRange.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static void checkRange(Object[] array, int idx) {
        if (idx < 0 || idx >= array.length) {
            throw new ArrayIndexOutOfBoundsException(idx);
        }
    }

    /**
     * EnsureCapacity.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T> T[] ensureCapacity(T[] array, int required) {
        if (required < 0) {
            throw new IllegalArgumentException("Capacity must not be negative");
        }

        return array.length <= required ? Arrays.copyOf(array, nextPowerOf2(required)) : array;
    }

    /**
     * Round up the argument to the next highest power of 2.
     *
     * @param v Value to round up.
     * @return Next closest power of 2.
     */
    public static int nextPowerOf2(int v) {
        if (v < 0) {
            throw new IllegalArgumentException("v must not be negative");
        }

        if (v == 0) {
            return 1;
        }

        return 1 << (32 - Integer.numberOfLeadingZeros(v - 1));
    }

    /**
     * Creates mapping to trim the fields.
     *
     * <p>To find a new index of element after trimming call {@code mapping.getTargetOpt(index)}.
     *
     * <p>To find an old index of element before trimming call {@code mapping.getSourceOpt(index)}.
     *
     * <p>This mapping can be used to adjust traits or aggregations, for example, when several fields have been truncated.
     * Assume the following scenario:
     * <pre>
     *     We got a plan like below:
     *
     *     Project(b, d)
     *       TableScan(t, Distribution(b:1, d:3)): RowType(a, b, c, d)
     *
     *     In order to decrease amount of occupied memory, we can trim
     *     all the fields we aren't interested in. But such a trimming
     *     should change and indexes in Distribution trait as well. The
     *     plan after trimming should looks like this:
     *
     *     Project(b, d)
     *       TableScan(t, Distribution(b:0, d:1)): RowType(b, d)
     * </pre>
     *
     * @param sourceSize A size of the source collection.
     * @param requiredElements A set of elements that should be preserved.
     * @return A mapping for desired elements.
     * @see org.apache.calcite.plan.RelTrait#apply(TargetMapping)
     * @see org.apache.calcite.rel.core.AggregateCall#transform(TargetMapping)
     */
    public static Mapping trimmingMapping(int sourceSize, ImmutableBitSet requiredElements) {
        Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, sourceSize, requiredElements.cardinality());
        for (Ord<Integer> ord : Ord.zip(requiredElements)) {
            mapping.set(ord.e, ord.i);
        }
        return mapping;
    }

    /**
     * Creates mapping from given projection.
     *
     * <p>Projection is a list of integers representing an index of element from source
     * at desired position.
     *
     * @param sourceSize Size of the source.
     * @param projection Desired projection.
     * @return Mapping for given projection.
     */
    public static Mapping projectedMapping(int sourceSize, ImmutableIntList projection) {
        Mapping result = Mappings.create(MappingType.INVERSE_SURJECTION, sourceSize, projection.size());
        for (int i = 0; i < projection.size(); i++) {
            result.set(projection.getInt(i), i);
        }
        return result;
    }

    /** Reads the value from given tuple according to provided type. */
    public static @Nullable Object readValue(InternalTuple tuple, NativeType nativeType, int fieldIndex) {
        switch (nativeType.spec()) {
            case BOOLEAN: return tuple.booleanValueBoxed(fieldIndex);
            case INT8: return tuple.byteValueBoxed(fieldIndex);
            case INT16: return tuple.shortValueBoxed(fieldIndex);
            case INT32: return tuple.intValueBoxed(fieldIndex);
            case INT64: return tuple.longValueBoxed(fieldIndex);
            case FLOAT: return tuple.floatValueBoxed(fieldIndex);
            case DOUBLE: return tuple.doubleValueBoxed(fieldIndex);
            case DECIMAL: return tuple.decimalValue(fieldIndex, ((DecimalNativeType) nativeType).scale());
            case UUID: return tuple.uuidValue(fieldIndex);
            case STRING: return tuple.stringValue(fieldIndex);
            case BYTES: return tuple.bytesValue(fieldIndex);
            case BITMASK: return tuple.bitmaskValue(fieldIndex);
            case NUMBER: return tuple.numberValue(fieldIndex);
            case DATE: return tuple.dateValue(fieldIndex);
            case TIME: return tuple.timeValue(fieldIndex);
            case DATETIME: return tuple.dateTimeValue(fieldIndex);
            case TIMESTAMP: return tuple.timestampValue(fieldIndex);
            default: throw new InvalidTypeException("Unknown element type: " + nativeType);
        }
    }

    /**
     * Returns the longest possible prefix of {@code seq} that could be form from provided {@code elems}.
     *
     * @param seq   Sequence.
     * @param elems Elems.
     * @return The longest possible prefix of {@code seq}.
     */
    public static IntList maxPrefix(ImmutableIntList seq, Collection<Integer> elems) {
        IntList res = new IntArrayList();

        IntOpenHashSet elems0 = new IntOpenHashSet(elems);

        int e;
        for (int i = 0; i < seq.size(); i++) {
            e = seq.getInt(i);
            if (!elems0.remove(e)) {
                break;
            }

            res.add(e);
        }

        return res;
    }


    /**
     * Quietly closes given object ignoring possible checked exception.
     *
     * @param obj Object to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable Object obj) {
        if (obj instanceof AutoCloseable) {
            try {
                ((AutoCloseable) obj).close();
            } catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /**
     * Provide mapping Native types to java classes.
     *
     * @param type Native type
     * @return Java corresponding class.
     */
    public static Class<?> nativeTypeToClass(NativeType type) {
        assert type != null;

        switch (type.spec()) {
            case BOOLEAN:
                return Boolean.class;

            case INT8:
                return Byte.class;

            case INT16:
                return Short.class;

            case INT32:
                return Integer.class;

            case INT64:
                return Long.class;

            case FLOAT:
                return Float.class;

            case DOUBLE:
                return Double.class;

            case NUMBER:
                return BigInteger.class;

            case DECIMAL:
                return BigDecimal.class;

            case UUID:
                return UUID.class;

            case STRING:
                return String.class;

            case BYTES:
                return byte[].class;

            case BITMASK:
                return BitSet.class;

            case DATE:
                return LocalDate.class;

            case TIME:
                return LocalTime.class;

            case DATETIME:
                return LocalDateTime.class;

            case TIMESTAMP:
                return Instant.class;

            default:
                throw new IllegalArgumentException("Unsupported type " + type.spec());
        }
    }

    /**
     * Provide mapping Native types to JDBC classes.
     *
     * @param type Native type
     * @return JDBC corresponding class.
     */
    public static Class<?> nativeTypeToJdbcClass(NativeType type) {
        assert type != null;

        switch (type.spec()) {
            case DATE:
                return java.sql.Date.class;
            case TIME:
                return java.sql.Time.class;
            case DATETIME:
            case TIMESTAMP:
                return java.sql.Timestamp.class;
            default:
                return nativeTypeToClass(type);
        }
    }

    /**
     * Gets the precision of this type. Returns {@link ColumnMetadata#UNDEFINED_PRECISION} if
     * precision is not applicable for this type.
     *
     * @return Precision for current type.
     */
    public static int nativeTypePrecision(NativeType type) {
        assert type != null;

        switch (type.spec()) {
            case INT8:
                return 3;

            case INT16:
                return 5;

            case INT32:
                return 10;

            case INT64:
                return 19;

            case FLOAT:
            case DOUBLE:
                return 15;

            case NUMBER:
                return ((NumberNativeType) type).precision();

            case DECIMAL:
                return ((DecimalNativeType) type).precision();

            case BOOLEAN:
            case UUID:
            case DATE:
                return UNDEFINED_PRECISION;

            case TIME:
            case DATETIME:
            case TIMESTAMP:
                return ((TemporalNativeType) type).precision();

            case BYTES:
            case STRING:
                return ((VarlenNativeType) type).length();

            case BITMASK:
                return ((BitmaskNativeType) type).bits();

            default:
                throw new IllegalArgumentException("Unsupported type " + type.spec());
        }
    }

    /**
     * Gets the scale of this type. Returns {@link ColumnMetadata#UNDEFINED_SCALE} if
     * scale is not valid for this type.
     *
     * @return number of digits of scale
     */
    public static int nativeTypeScale(NativeType type) {
        switch (type.spec()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case NUMBER:
                return 0;

            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case UUID:
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case BYTES:
            case STRING:
            case BITMASK:
                return UNDEFINED_SCALE;

            case DECIMAL:
                return ((DecimalNativeType) type).scale();

            default:
                throw new IllegalArgumentException("Unsupported type " + type.spec());
        }
    }

    /**
     * CompoundComparator.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T> Comparator<T> compoundComparator(Iterable<Comparator<T>> cmps) {
        return (r1, r2) -> {
            for (Comparator<T> cmp : cmps) {
                int result = cmp.compare(r1, r2);

                if (result != 0) {
                    return result;
                }
            }

            return 0;
        };
    }

    /**
     * Returns cluster that can be used only as a stub to keep query tree in the cache.
     *
     * <p>Any attempt to invoke operation involving cluster on a tree attached to this cluster
     * will result in an error.
     *
     * @return A stub of a cluster.
     */
    public static RelOptCluster emptyCluster() {
        return CLUSTER;
    }

    /**
     * Returns cluster that may be used to acquire metadata from a relation tree, but should not
     * be used for planning.
     *
     * @return A new cluster.
     */
    public static RelOptCluster cluster() {
        RelOptCluster emptyCluster = emptyCluster();

        RelOptCluster cluster = RelOptCluster.create(emptyCluster.getPlanner(), emptyCluster.getRexBuilder());

        cluster.setMetadataProvider(IgniteMetadata.METADATA_PROVIDER);
        cluster.setMetadataQuerySupplier(RelMetadataQueryEx::create);

        return cluster;
    }

    /**
     * Checks whether an implicit PK mode enabled or not.
     *
     * <p>Note: this mode is for test purpose only.
     *
     * @return A {@code true} if implicit pk mode is enabled, {@code false} otherwise.
     */
    public static boolean implicitPkEnabled() {
        return IgniteSystemProperties.getBoolean("IMPLICIT_PK_ENABLED", false);
    }

    /**
     * Returns a short version of a rule description.
     *
     * <p>Short description is used to match the rule to disable in DISABLE_RULE hint processor.
     *
     * @param rule A rule to derive description from.
     * @return A short description of the rule.
     */
    public static String shortRuleName(RelOptRule rule) {
        String ruleDescription = rule.toString();

        int pos = ruleDescription.indexOf('(');

        if (pos == -1) {
            return ruleDescription;
        }

        return ruleDescription.substring(0, pos);
    }

    /**
     * Returns a {@link SqlQueryType} for the given {@link SqlNode}.
     *
     * <p>If the given node is neither {@code QUERY}, nor {@code DDL}, nor {@code DML}, this method returns {@code null}.
     *
     * @param sqlNode An SQL node.
     * @return A query type.
     */
    @Nullable
    public static SqlQueryType getQueryType(SqlNode sqlNode) {
        SqlKind sqlKind = sqlNode.getKind();

        // Check for tx control types earlier, because COMMIT, ROLLBACK belongs to SqlKind.DDL
        if (sqlNode instanceof IgniteSqlStartTransaction || sqlNode instanceof IgniteSqlCommitTransaction) {
            return SqlQueryType.TX_CONTROL;
        }

        if (SqlKind.DDL.contains(sqlKind)) {
            return SqlQueryType.DDL;
        }

        switch (sqlKind) {
            case SELECT:
            case ORDER_BY:
            case WITH:
            case VALUES:
            case UNION:
            case EXCEPT:
            case INTERSECT:
                return SqlQueryType.QUERY;

            case INSERT:
            case DELETE:
            case UPDATE:
            case MERGE:
                return SqlQueryType.DML;

            case EXPLAIN:
                return SqlQueryType.EXPLAIN;

            default:
                return null;
        }
    }

    /**
     * Computes the least restrictive type among the provided inputs and
     * adds a projection where a cast to the inferred type is needed.
     *
     * @param inputs Input relational expressions.
     * @param cluster Cluster.
     * @param traits Traits of relational expression.
     * @return Converted inputs.
     */
    public static List<RelNode> castInputsToLeastRestrictiveTypeIfNeeded(List<RelNode> inputs, RelOptCluster cluster, RelTraitSet traits) {
        List<RelDataType> inputRowTypes = inputs.stream()
                .map(RelNode::getRowType)
                .collect(Collectors.toList());

        // Output type of a set operator is equal to leastRestrictive(inputTypes) (see SetOp::deriveRowType)

        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RelDataType resultType = typeFactory.leastRestrictive(inputRowTypes);
        if (resultType == null) {
            throw new IllegalArgumentException("Cannot compute compatible row type for arguments to set op: " + inputRowTypes);
        }

        // Check output type of each input, if input's type does not match the result type,
        // then add a projection with casts for non-matching fields.

        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RelNode> actualInputs = new ArrayList<>(inputs.size());

        for (RelNode input : inputs) {
            RelDataType inputRowType = input.getRowType();

            // We can ignore nullability because it is always safe to convert from
            // ROW (T1 nullable, T2 not nullable) to ROW (T1 nullable, T2 nullable)
            // and leastRestrictive does exactly that.
            if (SqlTypeUtil.equalAsStructSansNullability(typeFactory, resultType, inputRowType, null)) {
                actualInputs.add(input);

                continue;
            }

            List<RexNode> exprs = new ArrayList<>(inputRowType.getFieldCount());

            for (int i = 0; i < resultType.getFieldCount(); i++) {
                RelDataType fieldType = inputRowType.getFieldList().get(i).getType();
                RelDataType outFieldType = resultType.getFieldList().get(i).getType();
                RexNode ref = rexBuilder.makeInputRef(input, i);

                if (fieldType.equals(outFieldType)) {
                    exprs.add(ref);
                } else {
                    RexNode expr = rexBuilder.makeCast(outFieldType, ref, true, false);
                    exprs.add(expr);
                }
            }

            actualInputs.add(new IgniteProject(cluster, traits, input, exprs, resultType));
        }

        return actualInputs;
    }

    /** Returns {@code true} if the specified properties allow multi-statement query execution. */
    public static boolean isMultiStatementQueryAllowed(SqlProperties properties) {
        Set<SqlQueryType> allowedTypes = properties.get(ALLOWED_QUERY_TYPES);

        return allowedTypes.contains(SqlQueryType.TX_CONTROL);
    }
}
