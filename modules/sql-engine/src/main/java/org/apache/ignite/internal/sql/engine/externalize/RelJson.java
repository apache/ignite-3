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

package org.apache.ignite.internal.sql.engine.externalize;

import static org.apache.calcite.sql.type.SqlTypeUtil.isApproximateNumeric;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.IgniteUtils.igniteClassLoader;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVariable;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsertKeyword;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction;
import org.apache.ignite.internal.sql.engine.trait.DistributionTrait;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Utilities for converting {@link RelNode} into JSON format.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
class RelJson {
    @SuppressWarnings("PublicInnerClass")
    @FunctionalInterface
    public interface RelFactory extends Function<RelInput, RelNode> {
        /** {@inheritDoc} */
        @Override
        RelNode apply(RelInput input);
    }

    private static final LoadingCache<String, RelFactory> FACTORIES_CACHE = Caffeine.newBuilder()
            .build(RelJson::relFactory);

    private static RelFactory relFactory(String typeName) {
        Class<?> clazz = null;

        if (!typeName.contains(".")) {
            for (String pckg : PACKAGES) {
                if ((clazz = classForName(pckg + typeName, true)) != null) {
                    break;
                }
            }
        }

        if (clazz == null) {
            clazz = classForName(typeName, false);
        }

        assert RelNode.class.isAssignableFrom(clazz);

        Constructor<RelNode> constructor;

        try {
            constructor = (Constructor<RelNode>) clazz.getConstructor(RelInput.class);
        } catch (NoSuchMethodException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "class does not have required constructor, "
                    + clazz + "(RelInput)");
        }

        BlockBuilder builder = new BlockBuilder();
        ParameterExpression input = Expressions.parameter(RelInput.class);
        builder.add(Expressions.new_(constructor, input));
        MethodDeclaration declaration = Expressions.methodDecl(
                Modifier.PUBLIC, RelNode.class, "apply", asList(input), builder.toBlock());
        return Commons.compile(RelFactory.class, Expressions.toString(asList(declaration), "\n", true));
    }

    private static final Map<String, Enum<?>> ENUM_BY_NAME;

    static {
        // Build a mapping from enum constants (e.g. LEADING) to the enum
        // that contains them (e.g. SqlTrimFunction.Flag). If there two
        // enum constants have the same name, the builder will throw.
        final Map<String, Enum<?>> enumByName = new HashMap<>();

        register(enumByName, JoinConditionType.class);
        register(enumByName, JoinType.class);
        register(enumByName, Direction.class);
        register(enumByName, NullDirection.class);
        register(enumByName, SqlTypeName.class);
        register(enumByName, SqlKind.class);
        register(enumByName, SqlSyntax.class);
        register(enumByName, SqlExplainFormat.class);
        register(enumByName, SqlExplainLevel.class);
        register(enumByName, SqlInsertKeyword.class);
        register(enumByName, SqlJsonConstructorNullClause.class);
        register(enumByName, SqlJsonQueryWrapperBehavior.class);
        register(enumByName, SqlJsonValueEmptyOrErrorBehavior.class);
        register(enumByName, SqlMatchRecognize.AfterOption.class);
        register(enumByName, SqlSelectKeyword.class);
        register(enumByName, SqlTrimFunction.Flag.class);
        register(enumByName, TimeUnitRange.class);

        ENUM_BY_NAME = Map.copyOf(enumByName);
    }

    private static void register(Map<String, Enum<?>> map, Class<? extends Enum> aclass) {
        String preffix = aclass.getSimpleName() + "#";
        for (Enum enumConstant : aclass.getEnumConstants()) {
            map.put(preffix + enumConstant.name(), enumConstant);
        }
    }

    private static Class<?> classForName(String typeName, boolean skipNotFound) {
        try {
            return IgniteUtils.forName(typeName, igniteClassLoader());
        } catch (ClassNotFoundException e) {
            if (!skipNotFound) {
                throw new IgniteInternalException(INTERNAL_ERR, "RelJson unable to load type: " + typeName);
            }
        }

        return null;
    }

    private static final List<String> PACKAGES =
            List.of(
                    "org.apache.ignite.internal.sql.engine.rel.",
                    "org.apache.ignite.internal.sql.engine.rel.agg.",
                    "org.apache.ignite.internal.sql.engine.rel.set.",
                    "org.apache.calcite.rel.",
                    "org.apache.calcite.rel.core.",
                    "org.apache.calcite.rel.logical.",
                    "org.apache.calcite.adapter.jdbc.",
                    "org.apache.calcite.adapter.jdbc.JdbcRules$");

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    RelJson() {
    }

    Function<RelInput, RelNode> factory(String type) {
        return FACTORIES_CACHE.get(type);
    }

    String classToTypeName(Class<? extends RelNode> cls) {
        if (IgniteRel.class.isAssignableFrom(cls)) {
            return cls.getSimpleName();
        }

        String canonicalName = cls.getName();
        for (String pckg : PACKAGES) {
            if (canonicalName.startsWith(pckg)) {
                String remaining = canonicalName.substring(pckg.length());
                if (remaining.indexOf('.') < 0 && remaining.indexOf('$') < 0) {
                    return remaining;
                }
            }
        }
        return canonicalName;
    }

    Object toJson(Object value) {
        if (value == null
                || value instanceof Number
                || value instanceof String
                || value instanceof Boolean
                || value instanceof UUID) {
            return value;
        } else if (value instanceof Enum) {
            return toJson((Enum) value);
        } else if (value instanceof RexNode) {
            return toJson((RexNode) value);
        } else if (value instanceof RexWindow) {
            return toJson((RexWindow) value);
        } else if (value instanceof RexFieldCollation) {
            return toJson((RexFieldCollation) value);
        } else if (value instanceof RexWindowBound) {
            return toJson((RexWindowBound) value);
        } else if (value instanceof CorrelationId) {
            return toJson((CorrelationId) value);
        } else if (value instanceof List) {
            List<Object> list = list();
            for (Object o : (Iterable) value) {
                list.add(toJson(o));
            }
            return list;
        } else if (value instanceof ImmutableBitSet) {
            List<Object> list = list();
            for (Integer integer : (Iterable<Integer>) value) {
                list.add(toJson(integer));
            }
            return list;
        } else if (value instanceof Set) {
            Set<Object> set = set();
            for (Object o : (Iterable) value) {
                set.add(toJson(o));
            }
            return set;
        } else if (value instanceof DistributionTrait) {
            return toJson((DistributionTrait) value);
        } else if (value instanceof AggregateCall) {
            return toJson((AggregateCall) value);
        } else if (value instanceof RelCollationImpl) {
            return toJson((RelCollationImpl) value);
        } else if (value instanceof RelDataType) {
            return toJson((RelDataType) value);
        } else if (value instanceof RelDataTypeField) {
            return toJson((RelDataTypeField) value);
        } else if (value instanceof ByteString) {
            return toJson((ByteString) value);
        } else if (value instanceof SearchBounds) {
            return toJson((SearchBounds) value);
        } else {
            throw new UnsupportedOperationException("type not serializable: "
                    + value + " (type " + value.getClass().getCanonicalName() + ")");
        }
    }

    private Object toJson(ByteString val) {
        return val.toString();
    }

    private Object toJson(Enum<?> enum0) {
        String key = enum0.getDeclaringClass().getSimpleName() + "#" + enum0.name();

        if (ENUM_BY_NAME.get(key) == enum0) {
            return key;
        }

        Map<String, Object> map = map();
        map.put("class", enum0.getDeclaringClass().getName());
        map.put("name", enum0.name());
        return map;
    }

    private Object toJson(AggregateCall node) {
        Map<String, Object> map = map();
        map.put("agg", toJson(node.getAggregation()));
        map.put("type", toJson(node.getType()));
        map.put("distinct", node.isDistinct());
        map.put("operands", node.getArgList());
        map.put("filter", node.filterArg);
        map.put("name", node.getName());
        map.put("rexList", toJson(node.rexList));
        return map;
    }

    private Object toJson(RelDataType node) {
        if (node instanceof JavaType) {
            Map<String, Object> map = map();
            map.put("class", ((JavaType) node).getJavaClass().getName());
            if (node.isNullable()) {
                map.put("nullable", true);
            }

            return map;
        }
        if (node.isStruct()) {
            List<Object> list = list();
            for (RelDataTypeField field : node.getFieldList()) {
                list.add(toJson(field));
            }
            return list;
        } else if (node.getSqlTypeName() == SqlTypeName.ARRAY) {
            Map<String, Object> map = map();
            map.put("type", toJson(node.getSqlTypeName()));
            map.put("elementType", toJson(node.getComponentType()));
            return map;
        } else if (node.getSqlTypeName() == SqlTypeName.MAP) {
            Map<String, Object> map = map();
            map.put("type", toJson(node.getSqlTypeName()));
            map.put("keyType", toJson(node.getKeyType()));
            map.put("valueType", toJson(node.getValueType()));
            return map;
        } else {
            Map<String, Object> map = map();
            map.put("type", toJson(node.getSqlTypeName()));
            if (node.isNullable()) {
                map.put("nullable", true);
            }
            if (node.getSqlTypeName().allowsPrec()) {
                map.put("precision", node.getPrecision());
            }
            if (node.getSqlTypeName().allowsScale()) {
                map.put("scale", node.getScale());
            }
            if (node instanceof IgniteCustomType) {
                // In case of a custom data type we must store its name to correctly
                // deserialize it because we want to distinguish a custom type from ANY.
                IgniteCustomType customType = (IgniteCustomType) node;
                map.put("type", toJson(SqlTypeName.ANY));
                map.put("customType", customType.getCustomTypeName());
            }
            return map;
        }
    }

    private Object toJson(RelDataTypeField node) {
        Map<String, Object> map;
        if (node.getType().isStruct()) {
            map = map();
            map.put("fields", toJson(node.getType()));
        } else {
            map = (Map<String, Object>) toJson(node.getType());
        }
        map.put("name", node.getName());
        return map;
    }

    private Object toJson(CorrelationId node) {
        return node.getId();
    }

    private Object toJson(RexNode node) {
        // removes calls to SEARCH and the included Sarg and converts them to comparisons
        node = RexUtil.expandSearch(Commons.emptyCluster().getRexBuilder(), null, node);

        Map<String, Object> map;
        switch (node.getKind()) {
            case FIELD_ACCESS:
                map = map();
                RexFieldAccess fieldAccess = (RexFieldAccess) node;
                map.put("field", fieldAccess.getField().getName());
                map.put("expr", toJson(fieldAccess.getReferenceExpr()));

                return map;
            case LITERAL:
                RexLiteral literal = (RexLiteral) node;
                Object value = literal.getValue3();
                map = map();
                map.put("literal", toJson(value));
                map.put("type", toJson(node.getType()));

                return map;
            case INPUT_REF:
                map = map();
                map.put("input", ((RexSlot) node).getIndex());
                map.put("name", ((RexVariable) node).getName());

                return map;
            case DYNAMIC_PARAM:
                map = map();
                map.put("input", ((RexDynamicParam) node).getIndex());
                map.put("name", ((RexVariable) node).getName());
                map.put("type", toJson(node.getType()));
                map.put("dynamic", true);

                return map;
            case LOCAL_REF:
                map = map();
                map.put("input", ((RexSlot) node).getIndex());
                map.put("name", ((RexVariable) node).getName());
                map.put("type", toJson(node.getType()));

                return map;
            case CORREL_VARIABLE:
                map = map();
                map.put("correl", ((RexVariable) node).getName());
                map.put("type", toJson(node.getType()));

                return map;
            default:
                if (node instanceof RexCall) {
                    RexCall call = (RexCall) node;
                    map = map();
                    map.put("op", toJson(call.getOperator()));
                    List<Object> list = list();

                    for (RexNode operand : call.getOperands()) {
                        list.add(toJson(operand));
                    }

                    map.put("operands", list);
                    map.put("type", toJson(node.getType()));

                    if (call.getOperator() instanceof SqlFunction) {
                        if (((SqlFunction) call.getOperator()).getFunctionType().isUserDefined()) {
                            SqlOperator op = call.getOperator();
                            map.put("class", op.getClass().getName());
                            map.put("deterministic", op.isDeterministic());
                            map.put("dynamic", op.isDynamicFunction());
                        }
                    }

                    if (call instanceof RexOver) {
                        RexOver over = (RexOver) call;
                        map.put("distinct", over.isDistinct());
                        map.put("window", toJson(over.getWindow()));
                    }

                    return map;
                }
                throw new UnsupportedOperationException("unknown rex " + node);
        }
    }

    private Object toJson(RexWindow window) {
        Map<String, Object> map = map();
        if (!window.partitionKeys.isEmpty()) {
            map.put("partition", toJson(window.partitionKeys));
        }
        if (!window.orderKeys.isEmpty()) {
            map.put("order", toJson(window.orderKeys));
        }
        if (window.getLowerBound() == null) { // NOPMD
            // No ROWS or RANGE clause
        } else if (window.getUpperBound() == null) {
            if (window.isRows()) {
                map.put("rows-lower", toJson(window.getLowerBound()));
            } else {
                map.put("range-lower", toJson(window.getLowerBound()));
            }
        } else if (window.isRows()) {
            map.put("rows-lower", toJson(window.getLowerBound()));
            map.put("rows-upper", toJson(window.getUpperBound()));
        } else {
            map.put("range-lower", toJson(window.getLowerBound()));
            map.put("range-upper", toJson(window.getUpperBound()));
        }
        return map;
    }

    private Object toJson(DistributionTrait distribution) {
        Type type = distribution.getType();

        switch (type) {
            case ANY:
            case BROADCAST_DISTRIBUTED:
            case RANDOM_DISTRIBUTED:
            case SINGLETON:

                return type.shortName;
            case HASH_DISTRIBUTED:
                Map<String, Object> map = map();
                List<Object> keys = list();
                for (Integer key : distribution.getKeys()) {
                    keys.add(toJson(key));
                }

                map.put("func", distribution.function().name());
                map.put("keys", keys);

                if (distribution.isTableDistribution()) {
                    map.put("zoneId", distribution.zoneId());
                    map.put("tableId", distribution.tableId());
                    map.put("label", distribution.label());
                }

                return map;
            default:
                throw new AssertionError("Unexpected distribution type.");
        }
    }

    private Object toJson(RelCollationImpl node) {
        List<Object> list = list();
        for (RelFieldCollation fieldCollation : node.getFieldCollations()) {
            Map<String, Object> map = map();
            map.put("field", fieldCollation.getFieldIndex());
            map.put("direction", toJson(fieldCollation.getDirection()));
            map.put("nulls", toJson(fieldCollation.nullDirection));
            list.add(map);
        }
        return list;
    }

    private Object toJson(RexFieldCollation collation) {
        Map<String, Object> map = map();
        map.put("expr", toJson(collation.left));
        map.put("direction", toJson(collation.getDirection()));
        map.put("null-direction", toJson(collation.getNullDirection()));
        return map;
    }

    private Object toJson(RexWindowBound windowBound) {
        Map<String, Object> map = map();
        if (windowBound.isCurrentRow()) {
            map.put("type", "CURRENT_ROW");
        } else if (windowBound.isUnbounded()) {
            map.put("type", windowBound.isPreceding() ? "UNBOUNDED_PRECEDING" : "UNBOUNDED_FOLLOWING");
        } else {
            map.put("type", windowBound.isPreceding() ? "PRECEDING" : "FOLLOWING");
            map.put("offset", toJson(windowBound.getOffset()));
        }
        return map;
    }

    private Object toJson(SqlOperator operator) {
        // User-defined operators are not yet handled.
        Map map = map();
        map.put("name", operator.getName());
        map.put("kind", toJson(operator.kind));
        map.put("syntax", toJson(operator.getSyntax()));

        if (operator.getOperandTypeChecker() != null && operator.getAllowedSignatures() != null) {
            map.put("signature", toJson(operator.getAllowedSignatures()));
        }
        return map;
    }

    private Object toJson(SearchBounds val) {
        Map map = map();
        map.put("type", val.type().name());

        if (val instanceof ExactBounds) {
            map.put("bound", toJson(((ExactBounds) val).bound()));
        } else if (val instanceof MultiBounds) {
            map.put("bounds", toJson(((MultiBounds) val).bounds()));
        } else {
            assert val instanceof RangeBounds : val;

            RangeBounds val0 = (RangeBounds) val;

            map.put("lowerBound", val0.lowerBound() == null ? null : toJson(val0.lowerBound()));
            map.put("upperBound", val0.upperBound() == null ? null : toJson(val0.upperBound()));
            map.put("lowerInclude", val0.lowerInclude());
            map.put("upperInclude", val0.upperInclude());
        }

        return map;
    }

    private SearchBounds toSearchBound(RelInput input, Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        String type = (String) map.get("type");

        if (SearchBounds.Type.EXACT.name().equals(type)) {
            return new ExactBounds(null, toRex(input, map.get("bound")));
        } else if (SearchBounds.Type.MULTI.name().equals(type)) {
            return new MultiBounds(null, toSearchBoundList(input, (List<Map<String, Object>>) map.get("bounds")));
        } else if (SearchBounds.Type.RANGE.name().equals(type)) {
            return new RangeBounds(null,
                    toRex(input, map.get("lowerBound")),
                    toRex(input, map.get("upperBound")),
                    (Boolean) map.get("lowerInclude"),
                    (Boolean) map.get("upperInclude")
            );
        }

        throw new IllegalStateException("Unsupported search bound type: " + type);
    }

    List<SearchBounds> toSearchBoundList(RelInput input, List<Map<String, Object>> bounds) {
        if (bounds == null) {
            return null;
        }

        return bounds.stream().map(b -> toSearchBound(input, b)).collect(Collectors.toList());
    }

    RelCollation toCollation(List<Map<String, Object>> jsonFieldCollations) {
        if (jsonFieldCollations == null) {
            return RelCollations.EMPTY;
        }

        List<RelFieldCollation> fieldCollations = jsonFieldCollations.stream()
                .map(this::toFieldCollation)
                .collect(Collectors.toList());

        return RelCollations.of(fieldCollations);
    }

    IgniteDistribution toDistribution(Object distribution) {
        if (distribution instanceof String) {
            switch ((String) distribution) {
                case "single":
                    return IgniteDistributions.single();
                case "any":
                    return IgniteDistributions.any();
                case "broadcast":
                    return IgniteDistributions.broadcast();
                case "random":
                    return IgniteDistributions.random();
                default:
                    // NO-OP
            }
        }

        Map<String, Object> map = (Map<String, Object>) distribution;
        String functionName = (String) map.get("func");

        List<Integer> keys = (List<Integer>) map.get("keys");

        switch (functionName) {
            case "identity": {
                assert keys.size() == 1;

                return IgniteDistributions.identity(keys.get(0));
            }
            case "hash": {
                if (map.get("tableId") == null) {
                    return IgniteDistributions.hash(keys, DistributionFunction.hash());
                }

                int tableId = (int) map.get("tableId");
                int zoneId = (int) map.get("zoneId");
                String label = (String) map.get("label");

                return IgniteDistributions.affinity(keys, tableId, zoneId, label);
            }
            default: {
                throw new IllegalStateException("Unsupported distribution function: " + functionName);
            }
        }
    }

    RelDataType toType(RelDataTypeFactory typeFactory, Object o) {
        if (o instanceof List) {
            List<Map<String, Object>> jsonList = (List<Map<String, Object>>) o;
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (Map<String, Object> jsonMap : jsonList) {
                builder.add((String) jsonMap.get("name"), toType(typeFactory, jsonMap));
            }
            return builder.build();
        } else if (o instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) o;
            String clazz = (String) map.get("class");
            boolean nullable = Boolean.TRUE.equals(map.get("nullable"));

            if (clazz != null) {
                RelDataType type = typeFactory.createJavaType(classForName(clazz, false));

                if (nullable) {
                    type = typeFactory.createTypeWithNullability(type, true);
                }

                return type;
            }

            Object fields = map.get("fields");
            // IgniteCustomType: In case of a custom data type JSON must contain a name of that type.
            String customType = (String) map.get("customType");

            if (fields != null) {
                return toType(typeFactory, fields);
            } else {
                SqlTypeName sqlTypeName = toEnum(map.get("type"));
                Integer precision = (Integer) map.get("precision");
                Integer scale = (Integer) map.get("scale");
                RelDataType type;

                if (SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName)) {
                    TimeUnit startUnit = sqlTypeName.getStartUnit();
                    TimeUnit endUnit = sqlTypeName.getEndUnit();
                    type = typeFactory.createSqlIntervalType(
                            new SqlIntervalQualifier(startUnit, endUnit, SqlParserPos.ZERO));
                } else if (sqlTypeName == SqlTypeName.ARRAY) {
                    type = typeFactory.createArrayType(toType(typeFactory, map.get("elementType")), -1);
                } else if (sqlTypeName == SqlTypeName.MAP) {
                    type = typeFactory.createMapType(
                            toType(typeFactory, map.get("keyType")),
                            toType(typeFactory, map.get("valueType"))
                    );
                } else if (sqlTypeName == SqlTypeName.ANY && customType != null) {
                    type = ((IgniteTypeFactory) typeFactory).createCustomType(customType, precision);
                } else if (precision == null) {
                    type = typeFactory.createSqlType(sqlTypeName);
                } else if (scale == null) {
                    type = typeFactory.createSqlType(sqlTypeName, precision);
                } else {
                    type = typeFactory.createSqlType(sqlTypeName, precision, scale);
                }

                if (nullable) {
                    type = typeFactory.createTypeWithNullability(type, true);
                }

                return type;
            }
        } else {
            SqlTypeName sqlTypeName = toEnum(o);
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    RexNode toRex(RelInput relInput, Object o) {
        RelOptCluster cluster = relInput.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        if (o == null) {
            return null;
        } else if (o instanceof Map) {
            Map map = (Map) o;
            Map<String, Object> opMap = (Map) map.get("op");
            IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);
            if (opMap != null) {
                if (map.containsKey("class")) {
                    opMap.put("class", map.get("class"));
                }
                List operands = (List) map.get("operands");
                List<RexNode> rexOperands = toRexList(relInput, operands);
                Object jsonType = map.get("type");
                Map window = (Map) map.get("window");
                if (window != null) {
                    final SqlAggFunction operator = (SqlAggFunction) toOp(opMap);
                    final RelDataType type = toType(typeFactory, jsonType);
                    final List<RexNode> partitionKeys = window.containsKey("partition")
                            ? toRexList(relInput, (List) window.get("partition")) : new ArrayList<>();
                    List<RexFieldCollation> orderKeys = new ArrayList<>();
                    if (window.containsKey("order")) {
                        orderKeys = toRexFieldCollationList(relInput, (List) window.get("order"));
                    }
                    RexWindowBound lowerBound;
                    RexWindowBound upperBound;
                    boolean physical;
                    if (window.get("rows-lower") != null) {
                        lowerBound = toRexWindowBound(relInput, (Map) window.get("rows-lower"));
                        upperBound = toRexWindowBound(relInput, (Map) window.get("rows-upper"));
                        physical = true;
                    } else if (window.get("range-lower") != null) {
                        lowerBound = toRexWindowBound(relInput, (Map) window.get("range-lower"));
                        upperBound = toRexWindowBound(relInput, (Map) window.get("range-upper"));
                        physical = false;
                    } else {
                        // No ROWS or RANGE clause
                        lowerBound = null;
                        upperBound = null;
                        physical = false;
                    }
                    boolean distinct = (Boolean) map.get("distinct");
                    return rexBuilder.makeOver(type, operator, rexOperands, partitionKeys,
                            ImmutableList.copyOf(orderKeys), lowerBound, upperBound, physical,
                            true, false, distinct, false);
                } else {
                    SqlOperator operator = toOp(opMap);
                    RelDataType type;
                    if (jsonType != null) {
                        type = toType(typeFactory, jsonType);
                    } else {
                        type = rexBuilder.deriveReturnType(operator, rexOperands);
                    }
                    return rexBuilder.makeCall(type, operator, rexOperands);
                }
            }
            Integer input = (Integer) map.get("input");
            if (input != null) {
                // Check if it is a local ref.
                if (map.containsKey("type")) {
                    RelDataType type = toType(typeFactory, map.get("type"));
                    return Boolean.TRUE.equals(map.get("dynamic"))
                            ? rexBuilder.makeDynamicParam(type, input)
                            : rexBuilder.makeLocalRef(type, input);
                }

                List<RelNode> inputNodes = relInput.getInputs();
                int i = input;
                for (RelNode inputNode : inputNodes) {
                    RelDataType rowType = inputNode.getRowType();
                    if (i < rowType.getFieldCount()) {
                        RelDataTypeField field = rowType.getFieldList().get(i);
                        return rexBuilder.makeInputRef(field.getType(), input);
                    }
                    i -= rowType.getFieldCount();
                }
                throw new RuntimeException("input field " + input + " is out of range");
            }

            String field = (String) map.get("field");
            if (field != null) {
                Object jsonExpr = map.get("expr");
                RexNode expr = toRex(relInput, jsonExpr);
                return rexBuilder.makeFieldAccess(expr, field, true);
            }

            String correl = (String) map.get("correl");
            if (correl != null) {
                RelDataType type = toType(typeFactory, map.get("type"));
                return rexBuilder.makeCorrel(type, new CorrelationId(correl));
            }

            if (map.containsKey("literal")) {
                Object literal = map.get("literal");
                RelDataType type = toType(typeFactory, map.get("type"));

                if (literal == null) {
                    return rexBuilder.makeNullLiteral(type);
                }

                // RexBuilder can transform literal which holds exact numeric representation into E notation form.
                // I.e. 100 can be presented like 1E2 which is also correct form but can differs from serialized plan notation.
                // near "if" branch is only matters for fragments serialization\deserialization correctness check
                if ((literal instanceof Long || literal instanceof Integer) && isApproximateNumeric(type)) {
                    literal = new BigDecimal(((Number) literal).longValue());
                }

                if (literal instanceof BigInteger) {
                    // If the literal is a BigInteger, RexBuilder assumes it represents a long value
                    // within the valid range and converts it without checking the bounds. If the
                    // actual value falls outside the valid range, an overflow will occur, leading to
                    // incorrect results. To prevent this, we should convert BigInteger to BigDecimal
                    // so RexBuilder can handle the conversion correctly
                    literal = new BigDecimal((BigInteger) literal);
                }

                if (type.getSqlTypeName() == SqlTypeName.SYMBOL) {
                    literal = toEnum(literal);
                } else if (type.getSqlTypeName().getFamily() == SqlTypeFamily.BINARY) {
                    literal = toByteString(literal);
                } else if (type.getSqlTypeName().getFamily() == SqlTypeFamily.TIMESTAMP && literal instanceof Integer) {
                    literal = ((Integer) literal).longValue();
                } else if (type.getSqlTypeName() == SqlTypeName.UUID) {
                    assert literal instanceof String : literal;

                    literal = UUID.fromString((String) literal);
                }

                return rexBuilder.makeLiteral(literal, type, true);
            }

            throw new UnsupportedOperationException("cannot convert to rex " + o);
        } else if (o instanceof Boolean) {
            return rexBuilder.makeLiteral((Boolean) o);
        } else if (o instanceof String) {
            return rexBuilder.makeLiteral((String) o);
        } else if (o instanceof Number) {
            Number number = (Number) o;
            if (number instanceof Double || number instanceof Float) {
                return rexBuilder.makeApproxLiteral(
                        BigDecimal.valueOf(number.doubleValue()));
            } else {
                return rexBuilder.makeExactLiteral(
                        BigDecimal.valueOf(number.longValue()));
            }
        } else {
            throw new UnsupportedOperationException("cannot convert to rex " + o);
        }
    }

    SqlOperator toOp(Map<String, Object> map) {
        // in case different operator has the same kind, check with both name and kind.
        String name = map.get("name").toString();
        SqlKind sqlKind = toEnum(map.get("kind"));
        SqlSyntax sqlSyntax = toEnum(map.get("syntax"));
        String sig = (String) map.get("signature");
        Predicate signature = s -> sig == null || sig.equals(s);
        List<SqlOperator> operators = new ArrayList<>();

        FRAMEWORK_CONFIG.getOperatorTable().lookupOperatorOverloads(
                new SqlIdentifier(name, new SqlParserPos(0, 0)),
                null,
                sqlSyntax,
                operators,
                SqlNameMatchers.liberal()
        );

        for (SqlOperator operator : operators) {
            if (operator.kind == sqlKind && (operator.getOperandTypeChecker() == null || signature.test(operator.getAllowedSignatures()))) {
                return operator;
            }
        }

        // Fallback still need for IgniteSqlOperatorTable.EQUALS and so on operators, can be removed
        // after operandTypeChecker will be aligned
        for (SqlOperator operator : operators) {
            if (operator.kind == sqlKind) {
                return operator;
            }
        }

        String cls = (String) map.get("class");
        if (cls != null) {
            return AvaticaUtils.instantiatePlugin(SqlOperator.class, cls);
        }

        String message = format("Unknown or unexpected operator: name: {}, kind: {}, syntax: {}", name, sqlKind, sqlSyntax);
        throw new IllegalStateException(message);
    }

    <T> List<T> list() {
        return new ArrayList<>();
    }

    <T> Set<T> set() {
        return new LinkedHashSet<>();
    }

    <T> Map<String, T> map() {
        return new LinkedHashMap<>();
    }

    private <T extends Enum<T>> T toEnum(Object o) {
        if (o instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) o;
            String cls = (String) map.get("class");
            String name = map.get("name").toString();
            return Util.enumVal((Class<T>) classForName(cls, false), name);
        }

        assert o instanceof String && ENUM_BY_NAME.containsKey(o);

        String name = (String) o;
        return (T) ENUM_BY_NAME.get(name);
    }

    private ByteString toByteString(Object o) {
        assert o instanceof String;

        return ByteString.of((String) o, 16);
    }

    private RelFieldCollation toFieldCollation(Map<String, Object> map) {
        Integer field = (Integer) map.get("field");
        Direction direction = toEnum(map.get("direction"));
        NullDirection nullDirection = toEnum(map.get("nulls"));
        return new RelFieldCollation(field, direction, nullDirection);
    }

    private List<RexFieldCollation> toRexFieldCollationList(RelInput relInput, List<Map<String, Object>> order) {
        if (order == null) {
            return null;
        }

        List<RexFieldCollation> list = new ArrayList<>();
        for (Map<String, Object> o : order) {
            RexNode expr = toRex(relInput, o.get("expr"));
            Set<SqlKind> directions = new HashSet<>();
            if (toEnum(o.get("direction")) == Direction.DESCENDING) {
                directions.add(SqlKind.DESCENDING);
            }
            if (toEnum(o.get("null-direction")) == NullDirection.FIRST) {
                directions.add(SqlKind.NULLS_FIRST);
            } else {
                directions.add(SqlKind.NULLS_LAST);
            }
            list.add(new RexFieldCollation(expr, directions));
        }
        return list;
    }

    private RexWindowBound toRexWindowBound(RelInput input, Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        String type = (String) map.get("type");
        switch (type) {
            case "CURRENT_ROW":
                return RexWindowBounds.create(
                        SqlWindow.createCurrentRow(SqlParserPos.ZERO), null);
            case "UNBOUNDED_PRECEDING":
                return RexWindowBounds.create(
                        SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null);
            case "UNBOUNDED_FOLLOWING":
                return RexWindowBounds.create(
                        SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO), null);
            case "PRECEDING":
                RexNode precedingOffset = toRex(input, map.get("offset"));
                return RexWindowBounds.create(null,
                        input.getCluster().getRexBuilder().makeCall(
                                SqlWindow.PRECEDING_OPERATOR, precedingOffset));
            case "FOLLOWING":
                RexNode followingOffset = toRex(input, map.get("offset"));
                return RexWindowBounds.create(null,
                        input.getCluster().getRexBuilder().makeCall(
                                SqlWindow.FOLLOWING_OPERATOR, followingOffset));
            default:
                throw new UnsupportedOperationException("cannot convert type to rex window bound " + type);
        }
    }

    private List<RexNode> toRexList(RelInput relInput, List<?> operands) {
        List<RexNode> list = new ArrayList<>();
        for (Object operand : operands) {
            list.add(toRex(relInput, operand));
        }
        return list;
    }
}
