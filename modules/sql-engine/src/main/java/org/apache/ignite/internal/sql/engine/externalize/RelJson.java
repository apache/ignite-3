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

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.calcite.sql.type.SqlTypeUtil.isApproximateNumeric;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EnumSet;
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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
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
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVariable;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
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
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.sql.engine.prepare.bounds.ExactBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.MultiBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.RangeBounds;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction;
import org.apache.ignite.internal.sql.engine.trait.DistributionTrait;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities for converting {@link RelNode} into JSON format.
 */
@SuppressWarnings({"rawtypes", "unchecked", "MethodMayBeStatic"})
class RelJson {
    private static final ObjectMapper OBJECT_MAPPER = IgniteRelJsonUtils.OBJECT_MAPPER;

    private static final IgniteRelJsonEnumCache ENUM_BY_NAME = IgniteRelJsonEnumCache.builder()
            .register(JoinConditionType.class)
            .register(JoinType.class)
            .register(RexUnknownAs.class)
            .register(Direction.class)
            .register(NullDirection.class)
            .register(SqlTypeName.class)
            .register(SqlKind.class)
            .register(SqlSyntax.class)
            .register(SqlExplainFormat.class)
            .register(SqlExplainLevel.class)
            .register(SqlInsertKeyword.class)
            .register(SqlJsonConstructorNullClause.class)
            .register(SqlJsonQueryWrapperBehavior.class)
            .register(SqlJsonValueEmptyOrErrorBehavior.class)
            .register(SqlMatchRecognize.AfterOption.class)
            .register(SqlSelectKeyword.class)
            .register(SqlTrimFunction.Flag.class)
            .register(TimeUnitRange.class)
            .build();

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

    private static final IgniteRelJsonTypesCache TYPE_FACTORIES = new IgniteRelJsonTypesCache(PACKAGES);

    Function<RelInput, RelNode> factory(String type) {
        return TYPE_FACTORIES.factory(type);
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

    @Nullable
    Object toJson(@Nullable Object value) {
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
        } else if (value instanceof RelDistribution) {
            return toJson((RelDistribution) value);
        } else if (value instanceof Sarg) {
            return toJson((Sarg) value);
        } else if (value instanceof RangeSet) {
            return toJson((RangeSet) value);
        } else if (value instanceof Range) {
            return toJson((Range) value);
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

        if (ENUM_BY_NAME.getNullable(key) == enum0) {
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

    private <C extends Comparable<C>> Object toJson(Sarg<C> node) {
        Map<String, @Nullable Object> map = map();
        map.put("rangeSet", toJson(node.rangeSet));
        map.put("nullAs", toJson(node.nullAs));
        return map;
    }

    private <C extends Comparable<C>> List<List<String>> toJson(RangeSet<C> rangeSet) {
        List<List<String>> list = new ArrayList<>();
        try {
            RangeSets.forEach(rangeSet, RangeToJsonConverter.<C>instance().andThen(list::add));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize RangeSet: ", e);
        }
        return list;
    }

    /**
     * Serializes a {@link Range} that can be deserialized using
     * {@link org.apache.calcite.rel.externalize.RelJson#rangeFromJson(List, RelDataType)}.
     */
    private <C extends Comparable<C>> List<String> toJson(Range<C> range) {
        return RangeSets.map(range, RangeToJsonConverter.instance());
    }

    private Object toJson(RelDistribution relDistribution) {
        Map<String, @Nullable Object> map = map();
        map.put("type", relDistribution.getType().name());
        if (!relDistribution.getKeys().isEmpty()) {
            map.put("keys", relDistribution.getKeys());
        }
        return map;
    }

    private Object toJson(RelDataType node) {
        final Map<String, @Nullable Object> map = map();

        if (node instanceof JavaType) {
            map.put("class", ((JavaType) node).getJavaClass().getName());
            if (node.isNullable()) {
                map.put("nullable", true);
            }

        } else if (node.isStruct()) {
            List<Object> list = list();
            for (RelDataTypeField field : node.getFieldList()) {
                list.add(toJson(field));
            }
            map.put("fields", list);
            map.put("nullable", node.isNullable());

        } else if (node.getSqlTypeName() == SqlTypeName.ARRAY) {
            map.put("type", toJson(node.getSqlTypeName()));
            map.put("elementType", toJson(node.getComponentType()));

        } else if (node.getSqlTypeName() == SqlTypeName.MAP) {
            map.put("type", toJson(node.getSqlTypeName()));
            map.put("keyType", toJson(node.getKeyType()));
            map.put("valueType", toJson(node.getValueType()));

        } else {
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
        }
        return map;
    }

    private Object toJson(RelDataTypeField node) {
        Map<String, Object> map = (Map<String, Object>) toJson(node.getType());
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
            case DYNAMIC_PARAM:
                map = map();
                RexDynamicParam rexDynamicParam = (RexDynamicParam) node;
                RelDataType rdpType = rexDynamicParam.getType();
                map.put("input", rexDynamicParam.getIndex());
                map.put("name", ((RexVariable) node).getName());
                map.put("type", toJson(rdpType));
                map.put("dynamic", true);

                return map;
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

            case LAMBDA_REF:
                RexLambdaRef ref = (RexLambdaRef) node;
                map = map();
                map.put("index", ref.getIndex());
                map.put("name", ref.getName());
                map.put("type", toJson(ref.getType()));
                return map;

            case LAMBDA:
                RexLambda lambda = (RexLambda) node;
                map = map();
                final List<@Nullable Object> parameters = list();
                for (RexLambdaRef param : lambda.getParameters()) {
                    parameters.add(toJson(param));
                }
                map.put("op", "lambda");
                map.put("parameters", parameters);
                map.put("expression", toJson(lambda.getExpression()));
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
            RexNode offset =
                    requireNonNull(windowBound.getOffset(),
                            () -> "getOffset for window bound " + windowBound);
            map.put("offset", toJson(offset));
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

    @SuppressWarnings("DataFlowIssue") // Bounds fields are final, so null checks are reliable
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

            map.put("shouldComputeLower", val0.shouldComputeLower() == null || val0.shouldComputeLower().isAlwaysTrue()
                    ? null : toJson(val0.shouldComputeLower()));
            map.put("lowerBound", val0.lowerBound() == null ? null : toJson(val0.lowerBound()));
            map.put("shouldComputeUpper", val0.shouldComputeUpper() == null || val0.shouldComputeUpper().isAlwaysTrue()
                    ? null : toJson(val0.shouldComputeUpper()));
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
        RexNode literalTrue = input.getCluster().getRexBuilder().makeLiteral(true);

        if (SearchBounds.Type.EXACT.name().equals(type)) {
            return new ExactBounds(null, toRex(input, map.get("bound")));
        } else if (SearchBounds.Type.MULTI.name().equals(type)) {
            return new MultiBounds(null, toSearchBoundList(input, (List<Map<String, Object>>) map.get("bounds")));
        } else if (SearchBounds.Type.RANGE.name().equals(type)) {
            return new RangeBounds(null,
                    requireNonNullElse(toRex(input, map.get("shouldComputeLower")), literalTrue),
                    toRex(input, map.get("lowerBound")),
                    (Boolean) map.get("lowerInclude"),
                    requireNonNullElse(toRex(input, map.get("shouldComputeUpper")), literalTrue),
                    toRex(input, map.get("upperBound")),
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
                RelDataType type = typeFactory.createJavaType(IgniteRelJsonUtils.classForName(clazz));

                if (nullable) {
                    type = typeFactory.createTypeWithNullability(type, true);
                }

                return type;
            }

            Object fields = map.get("fields");

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
                        // Note: lower and upper bounds are non-nullable, so this branch is not reachable
                        lowerBound = null;
                        upperBound = null;
                        physical = false;
                    }

                    final RexWindowExclusion exclude;
                    if (window.get("exclude") != null) {
                        exclude = toRexWindowExclusion((Map) window.get("exclude"));
                    } else {
                        exclude = RexWindowExclusion.EXCLUDE_NO_OTHER;
                    }

                    boolean distinct = (Boolean) map.get("distinct");
                    return rexBuilder.makeOver(type, operator, rexOperands, partitionKeys,
                            ImmutableList.copyOf(orderKeys),
                            requireNonNull(lowerBound, "lowerBound"),
                            requireNonNull(upperBound, "upperBound"),
                            requireNonNull(exclude, "exclude"),
                            physical,
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

                if (literal instanceof Map
                        && ((Map<?, ?>) literal).containsKey("rangeSet")) {
                    Sarg sarg = sargFromJson((Map) literal, type);
                    return rexBuilder.makeSearchArgumentLiteral(sarg, type);
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

            if (map.containsKey("sargLiteral")) {
                Object sargObject = map.get("sargLiteral");
                if (sargObject == null) {
                    final RelDataType type = toType(typeFactory, map.get("type"));
                    return rexBuilder.makeNullLiteral(type);
                }
                final RelDataType type = toType(typeFactory, map.get("type"));
                Sarg sarg = sargFromJson((Map) sargObject, type);
                return rexBuilder.makeSearchArgumentLiteral(sarg, type);
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

    private static <C extends Comparable<C>> Sarg<C> sargFromJson(Map<String, Object> map, RelDataType type) {
        final String nullAs = requireNonNull((String) map.get("nullAs"), "nullAs");
        final List<List<String>> rangeSet =
                requireNonNull((List<List<String>>) map.get("rangeSet"), "rangeSet");
        return Sarg.of(ENUM_BY_NAME.get(nullAs),
                RelJson.<C>rangeSetFromJson(rangeSet, type));
    }

    /** Converts a JSON list to a {@link RangeSet} with supplied value typing. */
    private static <C extends Comparable<C>> RangeSet<C> rangeSetFromJson(
            List<List<String>> rangeSetsJson, RelDataType type) {
        final ImmutableRangeSet.Builder<C> builder = ImmutableRangeSet.builder();
        try {
            rangeSetsJson.forEach(list -> builder.add(rangeFromJson(list, type)));
        } catch (Exception e) {
            throw new RuntimeException("Error creating RangeSet from JSON: ", e);
        }
        return builder.build();
    }

    /**
     * Creates a {@link Range} from a JSON object.
     *
     * <p>The JSON object is as serialized using {@link #toJson(Range)},
     * e.g. {@code ["[", ")", 10, "-"]}.
     */
    private static <C extends Comparable<C>> Range<C> rangeFromJson(
            List<String> list, RelDataType type) {
        switch (list.get(0)) {
            case "all":
                return Range.all();
            case "atLeast":
                return Range.atLeast(rangeEndPointFromJson(list.get(1), type));
            case "atMost":
                return Range.atMost(rangeEndPointFromJson(list.get(1), type));
            case "greaterThan":
                return Range.greaterThan(rangeEndPointFromJson(list.get(1), type));
            case "lessThan":
                return Range.lessThan(rangeEndPointFromJson(list.get(1), type));
            case "singleton":
                return Range.singleton(rangeEndPointFromJson(list.get(1), type));
            case "closed":
                return Range.closed(rangeEndPointFromJson(list.get(1), type),
                        rangeEndPointFromJson(list.get(2), type));
            case "closedOpen":
                return Range.closedOpen(rangeEndPointFromJson(list.get(1), type),
                        rangeEndPointFromJson(list.get(2), type));
            case "openClosed":
                return Range.openClosed(rangeEndPointFromJson(list.get(1), type),
                        rangeEndPointFromJson(list.get(2), type));
            case "open":
                return Range.open(rangeEndPointFromJson(list.get(1), type),
                        rangeEndPointFromJson(list.get(2), type));
            default:
                throw new AssertionError("unknown range type " + list.get(0));
        }
    }

    private static <C extends Comparable<C>> C rangeEndPointFromJson(Object o, RelDataType type) {
        Exception e;
        try {
            Class clsType = determineRangeEndpointValueClass(type);
            return (C) OBJECT_MAPPER.readValue((String) o, clsType);
        } catch (JsonProcessingException ex) {
            e = ex;
        }
        throw new RuntimeException(
                "Error deserializing range endpoint (did not find compatible type): ",
                e);
    }

    private static Class determineRangeEndpointValueClass(RelDataType type) {
        SqlTypeName typeName = RexLiteral.strictTypeName(type);
        switch (typeName) {
            case DECIMAL:
                return BigDecimal.class;
            case DOUBLE:
                return Double.class;
            case CHAR:
                return NlsString.class;
            case BOOLEAN:
                return Boolean.class;
            case TIMESTAMP:
                return TimestampString.class;
            case DATE:
                return DateString.class;
            case TIME:
                return TimeString.class;
            default:
                throw new RuntimeException(
                        "Error deserializing range endpoint (did not find compatible type)");
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
                new SqlIdentifier(name, SqlParserPos.ZERO),
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

    @SuppressWarnings("DataFlowIssue")
    private <T extends Enum<T>> T toEnum(Object o) {
        if (o instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) o;
            String cls = (String) map.get("class");
            String name = map.get("name").toString();
            return Util.enumVal((Class<T>) IgniteRelJsonUtils.classForName(cls), name);
        }

        return ENUM_BY_NAME.resolveFrom(o);
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
            Set<SqlKind> directions = EnumSet.noneOf(SqlKind.class);
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

    private static @Nullable RexWindowExclusion toRexWindowExclusion(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        String type = (String) map.get("type");
        switch (type) {
            case "CURRENT_ROW":
                return RexWindowExclusion.EXCLUDE_CURRENT_ROW;
            case "GROUP":
                return RexWindowExclusion.EXCLUDE_GROUP;
            case "TIES":
                return RexWindowExclusion.EXCLUDE_TIES;
            case "NO OTHERS":
                return RexWindowExclusion.EXCLUDE_NO_OTHER;
            default:
                throw new UnsupportedOperationException("cannot convert " + type + " to rex window exclusion");
        }
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

    /**
     * Implementation of {@link RangeSets.Handler} that converts a {@link Range} event to a list of strings.
     *
     * @param <V> Range value type
     */
    private static class RangeToJsonConverter<V>
            implements RangeSets.Handler<@NonNull V, List<String>> {
        @SuppressWarnings("rawtypes")
        private static final RangeToJsonConverter INSTANCE = new RangeToJsonConverter<>();

        private static <C extends Comparable<C>> RangeToJsonConverter<C> instance() {
            return INSTANCE;
        }

        @Override
        public List<String> all() {
            return ImmutableList.of("all");
        }

        @Override
        public List<String> atLeast(@NonNull V lower) {
            return ImmutableList.of("atLeast", toJson(lower));
        }

        @Override
        public List<String> atMost(@NonNull V upper) {
            return ImmutableList.of("atMost", toJson(upper));
        }

        @Override
        public List<String> greaterThan(@NonNull V lower) {
            return ImmutableList.of("greaterThan", toJson(lower));
        }

        @Override
        public List<String> lessThan(@NonNull V upper) {
            return ImmutableList.of("lessThan", toJson(upper));
        }

        @Override
        public List<String> singleton(@NonNull V value) {
            return ImmutableList.of("singleton", toJson(value));
        }

        @Override
        public List<String> closed(@NonNull V lower, @NonNull V upper) {
            return ImmutableList.of("closed", toJson(lower), toJson(upper));
        }

        @Override
        public List<String> closedOpen(@NonNull V lower, @NonNull V upper) {
            return ImmutableList.of("closedOpen", toJson(lower), toJson(upper));
        }

        @Override
        public List<String> openClosed(@NonNull V lower, @NonNull V upper) {
            return ImmutableList.of("openClosed", toJson(lower), toJson(upper));
        }

        @Override
        public List<String> open(@NonNull V lower, @NonNull V upper) {
            return ImmutableList.of("open", toJson(lower), toJson(upper));
        }

        private static String toJson(Object o) {
            try {
                return OBJECT_MAPPER.writeValueAsString(o);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize Range endpoint: ", e);
            }
        }
    }
}
