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

import static org.apache.ignite.internal.sql.engine.util.BaseQueryContext.CLUSTER;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.RexExecutorImpl;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.prepare.IgniteConvertletTable;
import org.apache.ignite.internal.sql.engine.prepare.IgniteTypeCoercion;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlConformance;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlParser;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.sql.engine.trait.DistributionTraitDef;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteSystemProperties;
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
            RelCollationTraitDef.INSTANCE,
            DistributionTraitDef.INSTANCE
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
                                    .hintStrategy(IgniteHint.ENFORCE_JOIN_ORDER.name(), HintPredicates.JOIN)
                                    .hintStrategy(IgniteHint.DISABLE_RULE.name(), (hint, rel) -> true)
                                    .hintStrategy(IgniteHint.EXPAND_DISTINCT_AGG.name(), HintPredicates.AGGREGATE)
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
        return SqlTypeCoercionRule.instance(IgniteCustomAssigmentsRules.instance().getTypeMapping());
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
    public static <RowT> Object getFieldFromBiRows(RowHandler<RowT> hnd, int offset, RowT row1, RowT row2) {
        return offset < hnd.columnCount(row1) ? hnd.get(offset, row1) :
            hnd.get(offset - hnd.columnCount(row1), row2);
    }

    /**
     * Combines two lists.
     */
    public static <T> List<T> combine(List<T> left, List<T> right) {
        Set<T> set = new HashSet<>(left.size() + right.size());

        set.addAll(left);
        set.addAll(right);

        return new ArrayList<>(set);
    }

    /**
     * Intersects two lists.
     */
    public static <T> List<T> intersect(List<T> left, List<T> right) {
        if (nullOrEmpty(left) || nullOrEmpty(right)) {
            return Collections.emptyList();
        }

        return left.size() > right.size()
                ? intersect(new HashSet<>(right), left)
                : intersect(new HashSet<>(left), right);
    }

    /**
     * Intersects a set and a list.
     *
     * @return A List of unique entries that presented in both the given set and the given list.
     */
    public static <T> List<T> intersect(Set<T> set, List<T> list) {
        if (nullOrEmpty(set) || nullOrEmpty(list)) {
            return Collections.emptyList();
        }

        return list.stream()
                .filter(set::contains)
                .collect(Collectors.toList());
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
        return typeFactory(cluster());
    }

    /** Row-expression builder. **/
    public static RexBuilder rexBuilder() {
        return cluster().getRexBuilder();
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
     * ParametersMap.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param params Parameters.
     * @return Parameters map.
     */
    public static Map<String, Object> parametersMap(@Nullable Object[] params) {
        HashMap<String, Object> res = new HashMap<>();

        return params != null ? populateParameters(res, params) : res;
    }

    /**
     * Populates a provided map with given parameters.
     *
     * @param dst    Map to populate.
     * @param params Parameters.
     * @return Parameters map.
     */
    public static Map<String, Object> populateParameters(Map<String, Object> dst, @Nullable Object[] params) {
        if (!ArrayUtils.nullOrEmpty(params)) {
            for (int i = 0; i < params.length; i++) {
                dst.put("?" + i, params[i]);
            }
        }
        return dst;
    }

    /**
     * Close.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param o Object to close.
     */
    public static void close(Object o) throws Exception {
        if (o instanceof AutoCloseable) {
            ((AutoCloseable) o).close();
        }
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param o   Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception.
     */
    public static void close(Object o, IgniteLogger log) {
        if (o instanceof AutoCloseable) {
            try {
                ((AutoCloseable) o).close();
            } catch (Exception e) {
                log.warn("Failed to close resource", e);
            }
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
     * Negate.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <T> Predicate<T> negate(Predicate<T> p) {
        return p.negate();
    }

    /**
     * Creates mapping to trim the fields.
     *
     * <p>To find a new index of element after trimming call {@code mapping.getTargetOpt(index)}.
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
    public static Mappings.TargetMapping trimmingMapping(int sourceSize, ImmutableBitSet requiredElements) {
        Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, sourceSize, requiredElements.cardinality());
        for (Ord<Integer> ord : Ord.zip(requiredElements)) {
            mapping.set(ord.e, ord.i);
        }
        return mapping;
    }

    /**
     * Create a mapping to redo trimming made by {@link #trimmingMapping(int, ImmutableBitSet)}.
     *
     * <p>To find an old index of element before trimming call {@code mapping.getSourceOpt(index)}.
     *
     * <p>This mapping can be used to remap traits or aggregates back as if the trimming has never happened.
     *
     * @param sourceSize Count of elements in a non trimmed collection.
     * @param requiredElements Elements which were preserved during trimming.
     * @return A mapping to restore the original mapping.
     * @see #trimmingMapping(int, ImmutableBitSet)
     */
    public static Mappings.TargetMapping inverseTrimmingMapping(int sourceSize, ImmutableBitSet requiredElements) {
        return Mappings.invert(trimmingMapping(sourceSize, requiredElements).inverse());
    }

    /**
     * Produces new bitset setting bits according to the given mapping.
     *
     * <pre>
     * bitset:
     *   [0, 1, 4]
     * mapping:
     *   1 -> 0
     *   0 -> 1
     *   4 -> 3
     * result:
     *   [0, 1, 3]
     * </pre>
     *
     * @param bitset A bitset.
     * @param mapping Mapping to use.
     * @return  a transformed bit set.
     */
    public static ImmutableBitSet mapBitSet(ImmutableBitSet bitset, Mapping mapping) {
        ImmutableBitSet.Builder result = ImmutableBitSet.builder();

        int bitPos = bitset.nextSetBit(0);

        while (bitPos != -1) {
            int target = mapping.getTarget(bitPos);
            result.set(target);
            bitPos = bitset.nextSetBit(bitPos + 1);
        }

        return result.build();
    }


    /**
     * Checks if there is a such permutation of all {@code elems} that is prefix of provided {@code seq}.
     *
     * @param seq   Sequence.
     * @param elems Elems.
     * @return {@code true} if there is a permutation of all {@code elems} that is prefix of {@code seq}.
     */
    public static <T> boolean isPrefix(List<T> seq, Collection<T> elems) {
        Set<T> elems0 = new HashSet<>(elems);

        if (seq.size() < elems0.size()) {
            return false;
        }

        for (T e : seq) {
            if (!elems0.remove(e)) {
                return false;
            }

            if (elems0.isEmpty()) {
                break;
            }
        }

        return true;
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
     * NativeTypePrecision.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
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
                return -1;

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
     * NativeTypeScale.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
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
                return Integer.MIN_VALUE;

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

    public static RelOptCluster cluster() {
        return CLUSTER;
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
}
