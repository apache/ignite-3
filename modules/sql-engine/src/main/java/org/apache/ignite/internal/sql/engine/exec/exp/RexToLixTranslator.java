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

package org.apache.ignite.internal.sql.engine.exec.exp;

//CHECKSTYLE:OFF

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.tree.Expressions.constant;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CHAR_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OCTET_LENGTH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SEARCH;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUBSTRING;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.UPPER;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.CatchBlock;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.runtime.rtti.RuntimeTypeInformation;
import org.apache.calcite.schema.FunctionContext;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.util.IgniteMethod;
import org.apache.ignite.internal.sql.engine.util.Primitives;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.locationtech.jts.geom.Geometry;

/**
 * Translates {@link org.apache.calcite.rex.RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 * Changes in comparison with original code:
 * 1. Amends invocations for the following:
 *      EnumUtils.toInternal -> ConverterUtils.toInternal
 *      EnumUtils.internalTypes -> ConverterUtils.internalTypes
 *      EnumUtils.convert -> ConverterUtils.convert
 * 2. removed translateTableFunction method
 * 3. checkExpressionPadTruncate method - added operand as parameter to make ability to do cast to TIMESTMAP WITH LOCAL TIMEZONE
 *      Padding code is commented out (see pad = true) reverts changes from CALCITE-6350 in the same method).
 * 4. Added support for custom types conversion (see using of CustomTypesConversion class)
 * 5. Casts:
 *      Cast String to Time use own implementation IgniteMethod.UNIX_TIME_TO_STRING_PRECISION_AWARE
 *      Cast String to Timestamp use own implementation IgniteMethod.UNIX_TIMESTAMP_TO_STRING_PRECISION_AWARE
 *      Added support cast to decimal, see ConverterUtils.convertToDecimal
 *      Removed original casts to numeric types and used own ConverterUtils.convert
 *      Added pad-truncate from CHARACTER to INTERVAL types
 *      Added time-zone dependency for cast from CHARACTER types to TIMESTAMP WITH LOCAL TIMEZONE (see point 3)
 *      Cast VARCHAR to TIME is updated to use our implementation (see IgniteMethod.TIME_STRING_TO_TIME).
 *      Cast VARCHAR to DATE is updated to use our implementation (see IgniteMethod.DATE_STRING_TO_DATE).
 *      Cast TIMESTAMP to TIMESTAMP WITH LOCAL TIMEZONE use our implementation, see IgniteMethod.UNIX_TIMESTAMP_TO_STRING_PRECISION_AWARE
 *      Cast TIMESTAMP LTZ accepts FORMAT. (See IgniteMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE).
 *      Cast between TIME, TIMESTAMP amd TIMESTAMP_LTZ takes precision into account (see {@link IgniteMethod#ADJUST_TIMESTAMP_MILLIS}).
 * 6. Translate literals changes:
 *      DECIMAL use own implementation see IgniteSqlFunctions.class, "toBigDecimal"
 *      TIMESTAMP_WITH_LOCAL_TIME_ZONE use own implementation
 *      use Primitives.convertPrimitiveExact instead of primitive.number method
 *      Original branch that handles a UUID literal is commented out. A UUID literal is created from 2 long values.
 * 7. Reworked implementation of dynamic parameters:
 *      IgniteMethod.CONTEXT_GET_PARAMETER_VALUE instead of BuiltInMethod.DATA_CONTEXT_GET
 *      added conversation for Decimals
 * 8. Added parameter `RelDataType valueType` for implementRecursively method to do right datatype conversion
 * 9. Added parameter 'Format' to translateCastToTimestampWithLocalTimeZone.
 * 10. getConvertExpression Variant related code and if (targetType.getSqlTypeName() == SqlTypeName.ROW) are commented out.
 *     case DECIMAL: { and other numeric branches are commented aut (Some of them handle overflow checks, AI-3 has its own checks).
 * 11. scaleValue: code conversion from a non INTERVAL to INTERVAL is commented out (because it is buggy).      
 */
public class RexToLixTranslator implements RexVisitor<RexToLixTranslator.Result> {
  public static final Map<Method, SqlOperator> JAVA_TO_SQL_METHOD_MAP =
      ImmutableMap.<Method, SqlOperator>builder()
          .put(BuiltInMethod.STRING_TO_UPPER.method, UPPER)
          .put(BuiltInMethod.SUBSTRING.method, SUBSTRING)
          .put(BuiltInMethod.OCTET_LENGTH.method, OCTET_LENGTH)
          .put(BuiltInMethod.CHAR_LENGTH.method, CHAR_LENGTH)
          .put(BuiltInMethod.TRANSLATE3.method, TRANSLATE3)
          .build();

  final JavaTypeFactory typeFactory;
  final RexBuilder builder;
  private final @Nullable RexProgram program;
  final SqlConformance conformance;
  private final Expression root;
  final RexToLixTranslator.@Nullable InputGetter inputGetter;
  private final BlockBuilder list;
  private final @Nullable BlockBuilder staticList;
  private final @Nullable Function1<String, InputGetter> correlates;

  /**
   * Map from RexLiteral's variable name to its literal, which is often a
   * ({@link org.apache.calcite.linq4j.tree.ConstantExpression}))
   * It is used in the some {@code RexCall}'s implementors, such as
   * {@code ExtractImplementor}.
   *
   * @see #getLiteral
   * @see #getLiteralValue
   */
  private final Map<Expression, Expression> literalMap = new HashMap<>();

  /** For {@code RexCall}, keep the list of its operand's {@code Result}.
   * It is useful when creating a {@code CallImplementor}. */
  private final Map<RexCall, List<Result>> callOperandResultMap =
      new HashMap<>();

  /** Map from RexNode under specific storage type to its Result, to avoid
   * generating duplicate code. For {@code RexInputRef}, {@code RexDynamicParam}
   * and {@code RexFieldAccess}. */
  private final Map<Pair<RexNode, @Nullable Type>, Result> rexWithStorageTypeResultMap =
      new HashMap<>();

  /** Map from RexNode to its Result, to avoid generating duplicate code.
   * For {@code RexLiteral} and {@code RexCall}. */
  private final Map<RexNode, Result> rexResultMap = new HashMap<>();

  private @Nullable Type currentStorageType;

  private RexToLixTranslator(@Nullable RexProgram program,
      JavaTypeFactory typeFactory,
      Expression root,
      @Nullable InputGetter inputGetter,
      BlockBuilder list,
      @Nullable BlockBuilder staticList,
      RexBuilder builder,
      SqlConformance conformance,
      @Nullable Function1<String, InputGetter> correlates) {
    this.program = program; // may be null
    this.typeFactory = requireNonNull(typeFactory, "typeFactory");
    this.conformance = requireNonNull(conformance, "conformance");
    this.root = requireNonNull(root, "root");
    this.inputGetter = inputGetter;
    this.list = requireNonNull(list, "list");
    this.staticList = staticList;
    this.builder = requireNonNull(builder, "builder");
    this.correlates = correlates; // may be null
  }

  /**
   * Translates a {@link RexProgram} to a sequence of expressions and
   * declarations.
   *
   * @param program Program to be translated
   * @param typeFactory Type factory
   * @param conformance SQL conformance
   * @param list List of statements, populated with declarations
   * @param staticList List of member declarations
   * @param outputPhysType Output type, or null
   * @param root Root expression
   * @param inputGetter Generates expressions for inputs
   * @param correlates Provider of references to the values of correlated
   *                   variables
   * @return Sequence of expressions, optional condition
   */
  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, @Nullable BlockBuilder staticList,
      @Nullable PhysType outputPhysType, Expression root,
      InputGetter inputGetter, @Nullable Function1<String, InputGetter> correlates) {
    List<Type> storageTypes = null;
    if (outputPhysType != null) {
      final RelDataType rowType = outputPhysType.getRowType();
      storageTypes = new ArrayList<>(rowType.getFieldCount());
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        storageTypes.add(outputPhysType.getJavaFieldType(i));
      }
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter,
        list, staticList, new RexBuilder(typeFactory), conformance,  null)
        .setCorrelates(correlates)
        .translateList(program.getProjectList(), storageTypes);
  }

  @Deprecated // to be removed before 2.0
  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, @Nullable PhysType outputPhysType, Expression root,
      InputGetter inputGetter, @Nullable Function1<String, InputGetter> correlates) {
    return translateProjects(program, typeFactory, conformance, list, null,
        outputPhysType, root, inputGetter, correlates);
  }

/*    public static Expression translateTableFunction(JavaTypeFactory typeFactory,
      SqlConformance conformance, BlockBuilder list,
      Expression root, RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    final RexToLixTranslator translator =
        new RexToLixTranslator(null, typeFactory, root, null, list,
            null, new RexBuilder(typeFactory), conformance, null);
    return translator
        .translateTableFunction(rexCall, inputEnumerable, inputPhysType,
            outputPhysType);
  }*/

  /** Creates a translator for translating aggregate functions. */
  public static RexToLixTranslator forAggregation(JavaTypeFactory typeFactory,
      BlockBuilder list, @Nullable InputGetter inputGetter,
      SqlConformance conformance) {
    final ParameterExpression root = DataContext.ROOT;
    return new RexToLixTranslator(null, typeFactory, root, inputGetter, list,
        null, new RexBuilder(typeFactory), conformance, null);
  }

  Expression translate(RexNode expr) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
    return translate(expr, nullAs, null);
  }

  Expression translate(RexNode expr, @Nullable Type storageType) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs, storageType);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
      @Nullable Type storageType) {
    currentStorageType = storageType;
    final Result result = expr.accept(this);
    final Expression translated =
        requireNonNull(ConverterUtils.toInternal(result.valueVariable, storageType));
    // When we asked for not null input that would be stored as box, avoid unboxing
    if (RexImpTable.NullAs.NOT_POSSIBLE == nullAs
        && translated.type.equals(storageType)) {
      return translated;
    }
    return nullAs.handle(translated);
  }

  /**
   * Used for safe operators that return null if an exception is thrown.
   */
  private Expression expressionHandlingSafe(
      Expression body, boolean safe, RelDataType targetType) {
    return safe ? safeExpression(body, targetType) : body;
  }

  private Expression safeExpression(Expression body, RelDataType targetType) {
    final ParameterExpression e_ =
        Expressions.parameter(Exception.class, new BlockBuilder().newName("e"));

    // The type received for the targetType is never nullable.
    // But safe casts may return null
    RelDataType nullableTargetType = typeFactory.createTypeWithNullability(targetType, true);
    Expression result =
        Expressions.call(
            Expressions.lambda(
                Expressions.block(
                    Expressions.tryCatch(
                        Expressions.return_(null, body),
                        Expressions.catch_(e_,
                            Expressions.return_(null, constant(null)))))),
            BuiltInMethod.FUNCTION0_APPLY.method);
    // FUNCTION0 always returns Object, so we need a cast to the target type
    return ConverterUtils.convert(result, nullableTargetType);
  }

  Expression translateCast(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand,
      boolean safe,
      ConstantExpression format) {
    Expression convert = getConvertExpression(sourceType, targetType, operand, format);
    Expression convert2 = checkExpressionPadTruncate(convert, sourceType, targetType, operand);
    Expression convert3 = expressionHandlingSafe(convert2, safe, targetType);
    return scaleValue(sourceType, targetType, convert3);
  }

  private Expression getConvertExpression(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand,
      ConstantExpression format) {
    final Supplier<Expression> defaultExpression = () ->
            ConverterUtils.convert(operand, targetType);

//    if (sourceType.getSqlTypeName() == SqlTypeName.VARIANT) {
//      // Converting VARIANT to VARIANT uses the default conversion
//      if (targetType.getSqlTypeName() == SqlTypeName.VARIANT) {
//        return defaultExpression.get();
//      }
//      // Converting a VARIANT to any other type calls the Variant.cast method
//      // First cast operand to a VariantValue (it may be an Object)
//      Expression operandCast = Expressions.convert_(operand, VariantValue.class);
//      Expression cast =
//              Expressions.call(operandCast, BuiltInMethod.VARIANT_CAST.method,
//                      RuntimeTypeInformation.createExpression(targetType));
//      // The cast returns an Object, so we need a convert to the expected Java type
//      RelDataType nullableTarget = typeFactory.createTypeWithNullability(targetType, true);
//      return Expressions.convert_(cast, typeFactory.getJavaClass(nullableTarget));
//    }
//
//    if (targetType.getSqlTypeName() == SqlTypeName.ROW) {
//      assert sourceType.getSqlTypeName() == SqlTypeName.ROW;
//      List<RelDataTypeField> targetTypes = targetType.getFieldList();
//      List<RelDataTypeField> sourceTypes = sourceType.getFieldList();
//      assert targetTypes.size() == sourceTypes.size();
//      List<Expression> fields = new ArrayList<>();
//      for (int i = 0; i < targetTypes.size(); i++) {
//        RelDataTypeField targetField = targetTypes.get(i);
//        RelDataTypeField sourceField = sourceTypes.get(i);
//        Expression field = Expressions.arrayIndex(operand, Expressions.constant(i));
//        // In the generated Java code 'field' is an Object,
//        // we need to also cast it to the correct type to enable correct method dispatch in Java.
//        // We force the type to be nullable; this way, instead of (int) we get (Integer).
//        // Casting an object ot an int is not legal.
//        RelDataType nullableSourceFieldType =
//                typeFactory.createTypeWithNullability(sourceField.getType(), true);
//        Type javaType = typeFactory.getJavaClass(nullableSourceFieldType);
//        if (!javaType.getTypeName().equals("java.lang.Void")
//                && !nullableSourceFieldType.isStruct()) {
//          // Cannot cast to Void - this is the type of NULL literals.
//          field = Expressions.convert_(field, javaType);
//        }
//        Expression convert =
//                getConvertExpression(sourceField.getType(), targetField.getType(), field, format);
//        fields.add(convert);
//      }
//      return Expressions.call(BuiltInMethod.ARRAY.method, fields);
//    }

    switch (targetType.getSqlTypeName()) {
    case ARRAY:
      final RelDataType sourceDataType = sourceType.getComponentType();
      final RelDataType targetDataType = targetType.getComponentType();
      assert sourceDataType != null;
      assert targetDataType != null;
      final ParameterExpression parameter =
          Expressions.parameter(typeFactory.getJavaClass(sourceDataType), "root");
      Expression convert =
          getConvertExpression(sourceDataType, targetDataType, parameter, format);
      return Expressions.call(BuiltInMethod.LIST_TRANSFORM.method, operand,
          Expressions.lambda(Function1.class, convert, parameter));

    case VARIANT:
      // Converting any type to a VARIANT invokes the Variant constructor
      Expression rtti = RuntimeTypeInformation.createExpression(sourceType);
      Expression roundingMode = Expressions.constant(typeFactory.getTypeSystem().roundingMode());
      return Expressions.call(BuiltInMethod.VARIANT_CREATE.method, roundingMode, operand, rtti);
    case ANY:
      var toCustomType = CustomTypesConversion.INSTANCE.tryConvert(operand, targetType);
      return (toCustomType != null) ? toCustomType: operand;

    case VARBINARY:
    case BINARY:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        return Expressions.call(IgniteMethod.STRING_TO_BYTESTRING.method(), operand);

      default:
        return defaultExpression.get();
      }

    case GEOMETRY:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        return Expressions.call(BuiltInMethod.ST_GEOM_FROM_EWKT.method, operand);

      default:
        return defaultExpression.get();
      }

    case DATE:
      return translateCastToDate(sourceType, operand, format, defaultExpression);

    case TIME:
      return translateCastToTime(sourceType, targetType, operand, format, defaultExpression);

    case TIME_WITH_LOCAL_TIME_ZONE:
      return translateCastToTimeWithLocalTimeZone(sourceType, operand, defaultExpression);

    case TIMESTAMP:
      return translateCastToTimestamp(sourceType, targetType, operand, format, defaultExpression);

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return translateCastToTimestampWithLocalTimeZone(sourceType, targetType, operand, format, defaultExpression);

    case BOOLEAN:
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        return Expressions.call(BuiltInMethod.STRING_TO_BOOLEAN.method, operand);

      default:
        return defaultExpression.get();
      }
    case UUID:
      switch (sourceType.getSqlTypeName()) {
      case UUID:
        return operand;
      /*  
      case CHAR:
      case VARCHAR:
        return Expressions.call(BuiltInMethod.UUID_FROM_STRING.method, operand);
      case BINARY:
      case VARBINARY:
        return Expressions.call(BuiltInMethod.BINARY_TO_UUID.method, operand);
      */
      default:
        return defaultExpression.get();
      }
    case CHAR:
    case VARCHAR:
      final SqlIntervalQualifier interval =
          sourceType.getIntervalQualifier();
      switch (sourceType.getSqlTypeName()) {
      /*  
      case UUID:
        return Expressions.call(BuiltInMethod.UUID_TO_STRING.method, operand);
      */
      // If format string is supplied, return formatted date/time/timestamp
      case DATE:
        return RexImpTable.optimize2(operand, Expressions.isConstantNull(format)
            ? Expressions.call(BuiltInMethod.UNIX_DATE_TO_STRING.method, operand)
            : Expressions.call(
                Expressions.new_(
                    BuiltInMethod.FORMAT_DATE.method.getDeclaringClass()),
                BuiltInMethod.FORMAT_DATE.method, format, operand));

      case TIME:
        return RexImpTable.optimize2(operand, Expressions.isConstantNull(format)
            ? Expressions.call(
                IgniteMethod.UNIX_TIME_TO_STRING_PRECISION_AWARE.method(),
                operand,
                Expressions.constant(sourceType.getPrecision()))
            : Expressions.call(
                Expressions.new_(
                    BuiltInMethod.FORMAT_TIME.method.getDeclaringClass()),
                BuiltInMethod.FORMAT_TIME.method, format, operand));

      case TIME_WITH_LOCAL_TIME_ZONE:
        return RexImpTable.optimize2(operand, Expressions.isConstantNull(format)
            ? Expressions.call(BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_STRING.method, operand,
            Expressions.call(BuiltInMethod.TIME_ZONE.method, root))
            : Expressions.call(
                Expressions.new_(
                    BuiltInMethod.FORMAT_TIME.method.getDeclaringClass()),
                BuiltInMethod.FORMAT_TIME.method, format, operand));

      case TIMESTAMP:
        return RexImpTable.optimize2(operand, Expressions.isConstantNull(format)
            ? Expressions.call(
                IgniteMethod.UNIX_TIMESTAMP_TO_STRING_PRECISION_AWARE.method(),
            operand,
                Expressions.constant(sourceType.getPrecision()))
            : Expressions.call(
                Expressions.new_(
                    BuiltInMethod.FORMAT_TIMESTAMP.method.getDeclaringClass()),
                BuiltInMethod.FORMAT_TIMESTAMP.method, format, operand));

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return RexImpTable.optimize2(operand, Expressions.isConstantNull(format)
            ? Expressions.call(BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_STRING.method,
            operand, Expressions.call(BuiltInMethod.TIME_ZONE.method, root))
            : Expressions.call(
                Expressions.new_(
                    BuiltInMethod.FORMAT_TIMESTAMP.method.getDeclaringClass()),
                BuiltInMethod.FORMAT_TIMESTAMP.method, format, operand));

      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        return RexImpTable.optimize2(operand,
            Expressions.call(BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method,
                operand,
                Expressions.constant(
                    requireNonNull(interval, "interval").timeUnitRange)));

      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return RexImpTable.optimize2(operand,
            Expressions.call(BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method,
                operand,
                Expressions.constant(
                    requireNonNull(interval, "interval").timeUnitRange),
                Expressions.constant(
                    interval.getFractionalSecondPrecision(
                        typeFactory.getTypeSystem()))));

      case BOOLEAN:
        return RexImpTable.optimize2(operand,
            Expressions.call(BuiltInMethod.BOOLEAN_TO_STRING.method,
                operand));
      case BINARY:
      case VARBINARY:
        return RexImpTable.optimize2(
                operand,
                Expressions.call(IgniteMethod.BYTESTRING_TO_STRING.method(), operand));

      default:
        return defaultExpression.get();
      }

//    case DECIMAL: {
//      int precision = targetType.getPrecision();
//      int scale = targetType.getScale();
//      if (precision != RelDataType.PRECISION_NOT_SPECIFIED
//          && scale != RelDataType.SCALE_NOT_SPECIFIED) {
//        if (sourceType.getFamily() == SqlTypeFamily.CHARACTER) {
//          return Expressions.call(
//              BuiltInMethod.CHAR_DECIMAL_CAST_ROUNDING_MODE.method,
//              operand,
//              Expressions.constant(precision),
//              Expressions.constant(scale),
//              Expressions.constant(typeFactory.getTypeSystem().roundingMode()));
//        } else if (sourceType.getFamily() == SqlTypeFamily.INTERVAL_DAY_TIME) {
//          return Expressions.call(
//              BuiltInMethod.SHORT_INTERVAL_DECIMAL_CAST_ROUNDING_MODE.method,
//              operand,
//              Expressions.constant(precision),
//              Expressions.constant(scale),
//              Expressions.constant(sourceType.getSqlTypeName().getEndUnit().multiplier),
//              Expressions.constant(typeFactory.getTypeSystem().roundingMode()));
//        } else if (sourceType.getFamily() == SqlTypeFamily.INTERVAL_YEAR_MONTH) {
//          return Expressions.call(
//              BuiltInMethod.LONG_INTERVAL_DECIMAL_CAST_ROUNDING_MODE.method,
//              operand,
//              Expressions.constant(precision),
//              Expressions.constant(scale),
//              Expressions.constant(sourceType.getSqlTypeName().getEndUnit().multiplier),
//              Expressions.constant(typeFactory.getTypeSystem().roundingMode()));
//        } else if (sourceType.getSqlTypeName() == SqlTypeName.DECIMAL) {
//          // Cast from DECIMAL to DECIMAL, may adjust scale and precision.
//          return Expressions.call(
//              BuiltInMethod.DECIMAL_DECIMAL_CAST_ROUNDING_MODE.method,
//              operand,
//              Expressions.constant(precision),
//              Expressions.constant(scale),
//              Expressions.constant(typeFactory.getTypeSystem().roundingMode()));
//        } else if (SqlTypeName.INT_TYPES.contains(sourceType.getSqlTypeName())) {
//          // Cast from INTEGER to DECIMAL, check for overflow
//          return Expressions.call(
//              BuiltInMethod.INTEGER_DECIMAL_CAST_ROUNDING_MODE.method,
//              operand,
//              Expressions.constant(precision),
//              Expressions.constant(scale),
//              Expressions.constant(typeFactory.getTypeSystem().roundingMode()));
//        }  else if (SqlTypeName.APPROX_TYPES.contains(sourceType.getSqlTypeName())) {
//          // Cast from FLOAT/DOUBLE to DECIMAL
//          return Expressions.call(
//              BuiltInMethod.FP_DECIMAL_CAST_ROUNDING_MODE.method,
//              operand,
//              Expressions.constant(precision),
//              Expressions.constant(scale),
//              Expressions.constant(typeFactory.getTypeSystem().roundingMode()));
//        }
//      }
//      return defaultExpression.get();
//    }
//    case BIGINT:
//    case INTEGER:
//    case TINYINT:
//    case SMALLINT: {
//      if (SqlTypeName.NUMERIC_TYPES.contains(sourceType.getSqlTypeName())) {
//        Type javaClass = typeFactory.getJavaClass(targetType);
//        Primitive primitive = Primitive.of(javaClass);
//        if (primitive == null) {
//          primitive = Primitive.ofBox(javaClass);
//        }
//        return Expressions.call(
//            BuiltInMethod.INTEGER_CAST_ROUNDING_MODE.method,
//            Expressions.constant(primitive),
//            operand, Expressions.constant(typeFactory.getTypeSystem().roundingMode()));
//      }
//      return defaultExpression.get();
//    }
//
    default:
      return defaultExpression.get();
    }
  }

  private static Expression checkExpressionPadTruncate(
      Expression operand,
      RelDataType sourceType,
      RelDataType targetType,
      Expression sourceOperand) {
    // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no
    // longer than n.
    boolean pad = false;
    boolean truncate = true;
    switch (targetType.getSqlTypeName()) {
    case CHAR:
    case BINARY:
      //pad = true;
      // // fall through
    case VARCHAR:
    case VARBINARY:
      final int targetPrecision = targetType.getPrecision();
      if (targetPrecision < 0) {
        return operand;
      }
      switch (sourceType.getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
        // If this is a widening cast, no need to truncate.
        final int sourcePrecision = sourceType.getPrecision();
        if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
            <= 0) {
          truncate = false;
        }
//        // If this is a narrowing cast, no need to pad.
//        // However, conversion from VARCHAR(N) to CHAR(N) still requires padding,
//        // because VARCHAR(N) does not represent the spaces explicitly,
//        // whereas CHAR(N) does.
//        if ((SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision) >= 0)
//            && (sourceType.getSqlTypeName() != SqlTypeName.VARCHAR)) {
//          pad = false;
//        }
        // If this is a widening cast, no need to pad.
        if (SqlTypeUtil.comparePrecision(sourcePrecision, targetPrecision)
            >= 0) {
          pad = false;
        }
        // fall through
      default:
        if (truncate || pad) {
          final Method method =
              pad ? BuiltInMethod.TRUNCATE_OR_PAD.method
                  : BuiltInMethod.TRUNCATE.method;
          return Expressions.call(method, operand,
              Expressions.constant(targetPrecision));
        }
        return operand;
      }

      // Checkstyle thinks that the previous branch should have a break, but it
      // is mistaken.
      // CHECKSTYLE: IGNORE 1
    case TIMESTAMP:
      int targetScale = targetType.getScale();
      if (targetScale == RelDataType.SCALE_NOT_SPECIFIED) {
        targetScale = 0;
      }
      if (targetScale < sourceType.getScale()) {
        return Expressions.call(BuiltInMethod.ROUND_LONG.method, operand,
            Expressions.constant((long) Math.pow(10, 3 - targetScale)));
      }
      return operand;

    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      final SqlTypeFamily family =
          requireNonNull(sourceType.getSqlTypeName().getFamily(),
              () -> "null SqlTypeFamily for " + sourceType + ", SqlTypeName "
                  + sourceType.getSqlTypeName());
      switch (family) {
      case NUMERIC:
        final BigDecimal multiplier =
            targetType.getSqlTypeName().getEndUnit().multiplier;
        final BigDecimal divider = BigDecimal.ONE;
        return RexImpTable.multiplyDivide(operand, multiplier, divider);
      case CHARACTER:
          SqlIntervalQualifier intervalQualifier = targetType.getIntervalQualifier();

          Method method = intervalQualifier.isYearMonth()
                ? IgniteMethod.PARSE_INTERVAL_YEAR_MONTH.method()
                : IgniteMethod.PARSE_INTERVAL_DAY_TIME.method();

        return Expressions.call(
                method,
                sourceOperand,
                Expressions.new_(SqlIntervalQualifier.class,
                        Expressions.constant(intervalQualifier.getStartUnit()),
                        Expressions.constant(intervalQualifier.getStartPrecisionPreservingDefault()),
                        Expressions.constant(intervalQualifier.getEndUnit()),
                        Expressions.constant(intervalQualifier.getFractionalSecondPrecisionPreservingDefault()),
                        Expressions.field(null, SqlParserPos.class, "ZERO")
                )
        );
      default:
        return operand;
      }

    default:
      return operand;
    }
  }

  private Expression translateCastToDate(RelDataType sourceType,
      Expression operand, ConstantExpression format,
      Supplier<Expression> defaultExpression) {

    switch (sourceType.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      // If format string is supplied, parse formatted string into date
      return Expressions.isConstantNull(format)
              ? Expressions.call(BuiltInMethod.STRING_TO_DATE.method, operand)
              : Expressions.call(IgniteMethod.DATE_STRING_TO_DATE.method(), operand, format);

    case TIMESTAMP:
      return
          Expressions.convert_(
              Expressions.call(BuiltInMethod.FLOOR_DIV.method,
                  operand, Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
              int.class);

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return
          RexImpTable.optimize2(
              operand, Expressions.call(
                  BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE.method,
                  operand,
                  Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));

    default:
      return defaultExpression.get();
    }
  }

  private Expression translateCastToTime(RelDataType sourceType, RelDataType targetType,
      Expression operand, ConstantExpression format, Supplier<Expression> defaultExpression) {

    switch (sourceType.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      // If format string is supplied, parse formatted string into time
      Expression result = Expressions.isConstantNull(format)
          ? Expressions.call(IgniteMethod.STRING_TO_TIME.method(), operand)
          : Expressions.call(IgniteMethod.TIME_STRING_TO_TIME.method(), operand, format);

      return adjustTimeMillis(sourceType, targetType, result);

    case TIME_WITH_LOCAL_TIME_ZONE:
      return
          RexImpTable.optimize2(
              operand, Expressions.call(
                  BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                  operand,
                  Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));

    case TIMESTAMP:
      return
          adjustTimeMillis(sourceType, targetType,
              Expressions.convert_(
                  Expressions.call(BuiltInMethod.FLOOR_MOD.method,
                      operand,
                      Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                  int.class)
          );

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return
          adjustTimeMillis(sourceType, targetType,
              RexImpTable.optimize2(
                  operand, Expressions.call(
                      BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME.method,
                      operand,
                      Expressions.call(BuiltInMethod.TIME_ZONE.method, root)))
          );

    case TIME:
      return adjustTimeMillis(sourceType, targetType, operand);

    default:
      return defaultExpression.get();
    }
  }

  private Expression translateCastToTimeWithLocalTimeZone(RelDataType sourceType,
      Expression operand, Supplier<Expression> defaultExpression) {

    switch (sourceType.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      return
          Expressions.call(BuiltInMethod.STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method, operand);

    case TIME:
      return
          Expressions.call(BuiltInMethod.TIME_STRING_TO_TIME_WITH_LOCAL_TIME_ZONE.method,
              RexImpTable.optimize2(operand,
                  Expressions.call(BuiltInMethod.UNIX_TIME_TO_STRING.method,
                      operand)),
              Expressions.call(BuiltInMethod.TIME_ZONE.method, root));

    case TIMESTAMP:
      return
          Expressions.call(BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
              RexImpTable.optimize2(operand,
                  Expressions.call(BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                      operand)),
              Expressions.call(BuiltInMethod.TIME_ZONE.method, root));

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return
          RexImpTable.optimize2(
              operand, Expressions.call(
                  BuiltInMethod
                      .TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME_WITH_LOCAL_TIME_ZONE
                      .method,
                  operand));

    default:
      return defaultExpression.get();
    }
  }

  private Expression translateCastToTimestamp(RelDataType sourceType, RelDataType targetType,
      Expression operand, ConstantExpression format, Supplier<Expression> defaultExpression) {

    switch (sourceType.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      // If format string is supplied, parse formatted string into timestamp
      Expression result = Expressions.isConstantNull(format)
          ? Expressions.call(IgniteMethod.TO_TIMESTAMP_EXACT.method(), Expressions.call(IgniteMethod.STRING_TO_TIMESTAMP.method(), operand))
          : Expressions.call(IgniteMethod.TIMESTAMP_STRING_TO_TIMESTAMP.method(), operand, format);

      return adjustTimestampMillis(sourceType, targetType, result);

    case DATE:
      return
          Expressions.multiply(Expressions.convert_(operand, long.class),
              Expressions.constant(DateTimeUtils.MILLIS_PER_DAY));

    case TIME:
      return
          adjustTimestampMillis(sourceType, targetType,
              Expressions.add(
                  Expressions.multiply(
                      Expressions.convert_(
                          Expressions.call(IgniteMethod.CURRENT_DATE.method(), root),
                          long.class),
                      Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                  Expressions.convert_(operand, long.class))
          );

    case TIME_WITH_LOCAL_TIME_ZONE:
      return
          RexImpTable.optimize2(
              operand, Expressions.call(
                  BuiltInMethod.TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                  Expressions.call(BuiltInMethod.UNIX_DATE_TO_STRING.method,
                      Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                  operand,
                  Expressions.call(BuiltInMethod.TIME_ZONE.method, root)));

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return
          adjustTimestampMillis(sourceType, targetType,
              RexImpTable.optimize2(
                  operand, Expressions.call(
                      BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                      operand,
                      Expressions.call(BuiltInMethod.TIME_ZONE.method, root)))
          );

    case TIMESTAMP:
      return adjustTimestampMillis(sourceType, targetType, operand);

    default:
      return defaultExpression.get();
    }
  }

  private Expression translateCastToTimestampWithLocalTimeZone(RelDataType sourceType, RelDataType targetType,
      Expression operand, ConstantExpression format, Supplier<Expression> defaultExpression) {

    switch (sourceType.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      // By default Calcite for this type requires that the time zone be explicitly specified.
      // Since this type implies a local timezone, its explicit indication seems redundant,
      // so we prohibit the user from explicitly setting a timezone.
      Expression getTimeZone = Expressions.call(BuiltInMethod.TIME_ZONE.method, root);

      Expression result;

      if (Expressions.isConstantNull(format)) {
        result = Expressions.call(
                IgniteMethod.TO_TIMESTAMP_LTZ_EXACT.method(),
                Expressions.call(BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method, operand, getTimeZone)
        );
      } else {
        result = Expressions.call(
                IgniteMethod.TO_TIMESTAMP_LTZ_EXACT.method(),
                Expressions.call(IgniteMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method(), operand, format, getTimeZone)
        );
      }

      return adjustTimestampMillis(sourceType, targetType, result);

    case DATE:
      return
              Expressions.call(
                      IgniteMethod.TO_TIMESTAMP_LTZ_EXACT.method(),
                      Expressions.call(BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                              RexImpTable.optimize2(operand,
                                      Expressions.call(
                                              BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                                              Expressions.multiply(
                                                      Expressions.convert_(operand, long.class),
                                                      Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)))),
                              Expressions.call(BuiltInMethod.TIME_ZONE.method, root))
              );

    case TIME:
      return
            Expressions.call(BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                RexImpTable.optimize2(operand,
                    Expressions.call(
                        IgniteMethod.UNIX_TIMESTAMP_TO_STRING_PRECISION_AWARE.method(),
                        Expressions.add(
                            Expressions.multiply(
                                Expressions.convert_(
                                    Expressions.call(IgniteMethod.CURRENT_DATE.method(), root),
                                    long.class),
                                Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)),
                            Expressions.convert_(operand, long.class)),
                        constant(targetType.getPrecision())
                    )),
                Expressions.call(BuiltInMethod.TIME_ZONE.method, root));

    case TIME_WITH_LOCAL_TIME_ZONE:
      return
          RexImpTable.optimize2(
              operand, Expressions.call(
                  BuiltInMethod
                      .TIME_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE
                      .method,
                  Expressions.call(BuiltInMethod.UNIX_DATE_TO_STRING.method,
                      Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)),
                  operand));

    case TIMESTAMP:
      return
              Expressions.call(
                      IgniteMethod.TO_TIMESTAMP_LTZ_EXACT.method(),
                      Expressions.call(BuiltInMethod.TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE.method,
                          RexImpTable.optimize2(operand,
                              Expressions.call(
                                      IgniteMethod.UNIX_TIMESTAMP_TO_STRING_PRECISION_AWARE.method(),
                                      operand,
                                      Expressions.constant(targetType.getPrecision()))),
                          Expressions.call(BuiltInMethod.TIME_ZONE.method, root))
              );

    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return adjustTimestampMillis(sourceType, targetType, operand);

    default:
      return defaultExpression.get();
    }
  }

  private static Expression adjustTimestampMillis(RelDataType sourceType, RelDataType targetType, Expression operand) {
    if (sourceType.getSqlTypeName() == SqlTypeName.VARCHAR
            || sourceType.getPrecision() > targetType.getPrecision()) {
        return Expressions.call(
                IgniteMethod.ADJUST_TIMESTAMP_MILLIS.method(),
                operand,
                constant(targetType.getPrecision())
      );
    }

    return operand;
  }

  private static Expression adjustTimeMillis(RelDataType sourceType, RelDataType targetType, Expression operand) {
    if (sourceType.getSqlTypeName() == SqlTypeName.VARCHAR
            || sourceType.getPrecision() > targetType.getPrecision()) {
          return Expressions.call(
                  IgniteMethod.ADJUST_TIME_MILLIS.method(),
                  operand,
                  constant(targetType.getPrecision())
        );
    }

    return operand;
  }

  /**
   * Handle checked Exceptions declared in Method. In such case,
   * method call should be wrapped in a try...catch block.
   * "
   *      final Type method_call;
   *      try {
   *        method_call = callExpr
   *      } catch (Exception e) {
   *        throw new RuntimeException(e);
   *      }
   * "
   */
  Expression handleMethodCheckedExceptions(Expression callExpr) {
    // Try statement
    ParameterExpression methodCall =
        Expressions.parameter(callExpr.getType(), list.newName("method_call"));
    list.add(Expressions.declare(Modifier.FINAL, methodCall, null));
    Statement st = Expressions.statement(Expressions.assign(methodCall, callExpr));
    // Catch Block, wrap checked exception in unchecked exception
    ParameterExpression e = Expressions.parameter(0, Exception.class, "e");
    Expression uncheckedException = Expressions.new_(RuntimeException.class, e);
    CatchBlock cb = Expressions.catch_(e, Expressions.throw_(uncheckedException));
    list.add(Expressions.tryCatch(st, cb));
    return methodCall;
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  public RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = requireNonNull(program, "program")
          .getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  /** Translates a literal.
   *
   * @throws ControlFlowException if literal is null but {@code nullAs} is
   * {@link org.apache.calcite.adapter.enumerable.RexImpTable.NullAs#NOT_POSSIBLE}.
   */
  public static Expression translateLiteral(
      RexLiteral literal,
      RelDataType type,
      JavaTypeFactory typeFactory,
      RexImpTable.NullAs nullAs) {
    if (literal.isNull()) {
      switch (nullAs) {
      case TRUE:
      case IS_NULL:
        return RexImpTable.TRUE_EXPR;
      case FALSE:
      case IS_NOT_NULL:
        return RexImpTable.FALSE_EXPR;
      case NOT_POSSIBLE:
        throw new ControlFlowException();
      case NULL:
      default:
        return RexImpTable.NULL_EXPR;
      }
    } else {
      switch (nullAs) {
      case IS_NOT_NULL:
        return RexImpTable.TRUE_EXPR;
      case IS_NULL:
        return RexImpTable.FALSE_EXPR;
      default:
        break;
      }
    }
    Type javaClass = typeFactory.getJavaClass(type);
    final Object value2;
    switch (literal.getType().getSqlTypeName()) {
    case DECIMAL:
      final BigDecimal bd = literal.getValueAs(BigDecimal.class);
      if (javaClass == float.class) {
        return Expressions.constant(bd, javaClass);
      } else if (javaClass == double.class) {
        return Expressions.constant(bd, javaClass);
      }
      assert javaClass == BigDecimal.class;
      return Expressions.new_(BigDecimal.class,
          Expressions.constant(
              requireNonNull(bd,
                  () -> "value for " + literal).toString()));
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      Object val = literal.getValueAs(Long.class);

      // Literal was parsed as UTC timestamp, now we need to adjust it to the client's time zone.
      return Expressions.call(
              IgniteMethod.SUBTRACT_TIMEZONE_OFFSET.method(),
              Expressions.constant(val, long.class),
              Expressions.call(BuiltInMethod.TIME_ZONE.method, DataContext.ROOT)
      );
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      value2 = literal.getValueAs(Integer.class);
      javaClass = int.class;
      break;
    case TIMESTAMP:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      value2 = literal.getValueAs(Long.class);
      javaClass = long.class;
      break;
    case CHAR:
    case VARCHAR:
      value2 = literal.getValueAs(String.class);
      break;
    /* case UUID:
      return Expressions.call(null, BuiltInMethod.UUID_FROM_STRING.method,
          Expressions.constant(literal.getValueAs(String.class))); */
    case BINARY:
    case VARBINARY:
      return Expressions.new_(
          ByteString.class,
          Expressions.constant(
              literal.getValueAs(byte[].class),
              byte[].class));
    case GEOMETRY:
      final Geometry geom =
          requireNonNull(literal.getValueAs(Geometry.class),
              () -> "getValueAs(Geometries.Geom) for " + literal);
      final String wkt = SpatialTypeFunctions.ST_AsWKT(geom);
      return Expressions.call(null, BuiltInMethod.ST_GEOM_FROM_EWKT.method,
          Expressions.constant(wkt));
    case SYMBOL:
      value2 =
          requireNonNull(literal.getValueAs(Enum.class),
              () -> "getValueAs(Enum.class) for " + literal);
      javaClass = value2.getClass();
      break;
    case UUID: {
      UUID value = literal.getValueAs(UUID.class);

      // Literal NULL is covered at the very beginning of this method.
      assert value != null;

      return Expressions.new_(
              UUID.class, constant(value.getMostSignificantBits()), constant(value.getLeastSignificantBits())
      );
    }
    default:
      final Primitive primitive = Primitive.ofBoxOr(javaClass);
      final Comparable value = literal.getValueAs(Comparable.class);
      if (primitive != null && value instanceof Number) {
        value2 = Primitives.convertPrimitiveExact(primitive, (Number) value);
      } else {
        value2 = value;
      }
    }
    return Expressions.constant(value2, javaClass);
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs) {
    return translateList(operandList, nullAs,
            ConverterUtils.internalTypes(operandList));
  }

  public List<Expression> translateList(
      List<RexNode> operandList,
      RexImpTable.NullAs nullAs,
      List<? extends @Nullable Type> storageTypes) {
    final List<Expression> list = new ArrayList<>();
    for (Pair<RexNode, ? extends @Nullable Type> e : Pair.zip(operandList, storageTypes)) {
      list.add(translate(e.left, nullAs, e.right));
    }
    return list;
  }

  /**
   * Translates the list of {@code RexNode}, using the default output types.
   * This might be suboptimal in terms of additional box-unbox when you use
   * the translation later.
   * If you know the java class that will be used to store the results, use
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator#translateList(java.util.List, java.util.List)}
   * version.
   *
   * @param operandList list of RexNodes to translate
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList) {
    return translateList(operandList, ConverterUtils.internalTypes(operandList));
  }

  /**
   * Translates the list of {@code RexNode}, while optimizing for output
   * storage.
   * For instance, if the result of translation is going to be stored in
   * {@code Object[]}, and the input is {@code Object[]} as well,
   * then translator will avoid casting, boxing, etc.
   *
   * @param operandList list of RexNodes to translate
   * @param storageTypes hints of the java classes that will be used
   *                     to store translation results. Use null to use
   *                     default storage type
   *
   * @return translated expressions
   */
  public List<Expression> translateList(List<? extends RexNode> operandList,
      @Nullable List<? extends @Nullable Type> storageTypes) {
    final List<Expression> list = new ArrayList<>(operandList.size());

    for (int i = 0; i < operandList.size(); i++) {
      RexNode rex = operandList.get(i);
      Type desiredType = null;
      if (storageTypes != null) {
        desiredType = storageTypes.get(i);
      }
      final Expression translate = translate(rex, desiredType);
      list.add(translate);
      // desiredType is still a hint, thus we might get any kind of output
      // (boxed or not) when hint was provided.
      // It is favourable to get the type matching desired type
      if (desiredType == null && !isNullable(rex)) {
        assert !Primitive.isBox(translate.getType())
            : "Not-null boxed primitive should come back as primitive: "
            + rex + ", " + translate.getType();
      }
    }
    return list;
  }

/*  private Expression translateTableFunction(RexCall rexCall, Expression inputEnumerable,
      PhysType inputPhysType, PhysType outputPhysType) {
    assert rexCall.getOperator() instanceof SqlWindowTableFunction;
    TableFunctionCallImplementor implementor =
        RexImpTable.INSTANCE.get((SqlWindowTableFunction) rexCall.getOperator());
    if (implementor == null) {
      throw Util.needToImplement("implementor of " + rexCall.getOperator().getName());
    }
    return implementor.implement(
        this, inputEnumerable, rexCall, inputPhysType, outputPhysType);
  }*/

  public static Expression translateCondition(RexProgram program,
      JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter,
      Function1<String, InputGetter> correlates, SqlConformance conformance, Expression root) {
    RexLocalRef condition = program.getCondition();
    if (condition == null) {
      return RexImpTable.TRUE_EXPR;
    }
    RexToLixTranslator translator =
        new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
            null, new RexBuilder(typeFactory), conformance, null);
    translator = translator.setCorrelates(correlates);
    return translator.translate(
        condition,
        RexImpTable.NullAs.FALSE);
  }

  /** Returns whether an expression is nullable.
   *
   * @param e Expression
   * @return Whether expression is nullable
   */
  public boolean isNullable(RexNode e) {
    return e.getType().isNullable();
  }

  public RexToLixTranslator setBlock(BlockBuilder list) {
    if (list == this.list) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        staticList, builder, conformance, correlates);
  }

  public RexToLixTranslator setCorrelates(
      @Nullable Function1<String, InputGetter> correlates) {
    if (this.correlates == correlates) {
      return this;
    }
    return new RexToLixTranslator(program, typeFactory, root, inputGetter, list,
        staticList, builder, conformance, correlates);
  }

  public Expression getRoot() {
    return root;
  }

  /** If an expression is a {@code NUMERIC} derived from an {@code INTERVAL},
   * scales it appropriately; returns the operand unchanged if the conversion
   * is not from {@code INTERVAL} to {@code NUMERIC}.
   * Does <b>not</b> scale values of type DECIMAL, these are expected
   * to be already scaled. */
  private static Expression scaleValue(
      RelDataType sourceType,
      RelDataType targetType,
      Expression operand) {
    final SqlTypeFamily targetFamily = targetType.getSqlTypeName().getFamily();
    final SqlTypeFamily sourceFamily = sourceType.getSqlTypeName().getFamily();
    if (targetFamily == SqlTypeFamily.NUMERIC
        // multiplyDivide cannot handle DECIMALs, but for DECIMAL
        // target types the result is already scaled.
        && targetType.getSqlTypeName() != SqlTypeName.DECIMAL
        && (sourceFamily == SqlTypeFamily.INTERVAL_YEAR_MONTH
            || sourceFamily == SqlTypeFamily.INTERVAL_DAY_TIME)) {
      // Scale to the given field.
      final BigDecimal multiplier = BigDecimal.ONE;
      final BigDecimal divider =
          sourceType.getSqlTypeName().getEndUnit().multiplier;
      return RexImpTable.multiplyDivide(operand, multiplier, divider);
    }
    /* https://issues.apache.org/jira/browse/CALCITE-6751 CAST( CHAR_LENGTH('abc') AS INTERVAL DAY) = 3 * DayMillis  
    if (SqlTypeName.INTERVAL_TYPES.contains(targetType.getSqlTypeName())
        && !SqlTypeName.INTERVAL_TYPES.contains(sourceType.getSqlTypeName())) {
      // Conversion between intervals is only allowed if the intervals have the same type,
      // and then it should be a no-op.
      final BigDecimal multiplier = targetType.getSqlTypeName().getEndUnit().multiplier;
      final BigDecimal divider = BigDecimal.ONE;
      return RexImpTable.multiplyDivide(operand, multiplier, divider);
    }*/ 
    return operand;
  }

  /**
   * Visit {@code RexInputRef}. If it has never been visited
   * under current storage type before, {@code RexToLixTranslator}
   * generally produces three lines of code.
   *
   * <p>For example, when visiting a column (named commission) in
   * table Employee, the generated code snippet is:
   *
   * <blockquote><pre>{@code
   * final Employee current = (Employee) inputEnumerator.current();
   * final Integer input_value = current.commission;
   * final boolean input_isNull = input_value == null;
   * }</pre></blockquote>
   */
  @Override public Result visitInputRef(RexInputRef inputRef) {
    final Pair<RexNode, @Nullable Type> key = Pair.of(inputRef, currentStorageType);
    // If the RexInputRef has been visited under current storage type already,
    // it is not necessary to visit it again, just return the result.
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    // Generate one line of code to get the input, e.g.,
    // "final Employee current =(Employee) inputEnumerator.current();"
    final Expression valueExpression =
        requireNonNull(inputGetter, "inputGetter")
            .field(list, inputRef.getIndex(), currentStorageType);

    // Generate one line of code for the value of RexInputRef, e.g.,
    // "final Integer input_value = current.commission;"
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(), list.newName("input_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));

    // Generate one line of code to check whether RexInputRef is null, e.g.,
    // "final boolean input_isNull = input_value == null;"
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("input_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    final Result result = new Result(isNullVariable, valueVariable);

    // Cache <RexInputRef, currentStorageType>'s result
    // Note: EnumerableMatch's PrevInputGetter changes index each time,
    // it is not right to reuse the result under such case.
    //if (!(inputGetter instanceof EnumerableMatch.PrevInputGetter)) {
      rexWithStorageTypeResultMap.put(key, result);
    //}
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitLambdaRef(RexLambdaRef ref) {
    final ParameterExpression valueVariable =
        Expressions.parameter(
            typeFactory.getJavaClass(ref.getType()), ref.getName());

    // Generate one line of code to check whether lambdaRef is null, e.g.,
    // "final boolean input_isNull = $0 == null;"
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("input_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitLocalRef(RexLocalRef localRef) {
    return deref(localRef).accept(this);
  }

  /**
   * Visit {@code RexLiteral}. If it has never been visited before,
   * {@code RexToLixTranslator} will generate two lines of code. For example,
   * when visiting a primitive int (10), the generated code snippet is:
   * {@code
   *   final int literal_value = 10;
   *   final boolean literal_isNull = false;
   * }
   */
  @Override public Result visitLiteral(RexLiteral literal) {
    // If the RexLiteral has been visited already, just return the result
    if (rexResultMap.containsKey(literal)) {
      return rexResultMap.get(literal);
    }
    // Generate one line of code for the value of RexLiteral, e.g.,
    // "final int literal_value = 10;"
    final Expression valueExpression = literal.isNull()
        // Note: even for null literal, we can't loss its type information
        ? getTypedNullLiteral(literal)
        : translateLiteral(literal, literal.getType(),
            typeFactory, RexImpTable.NullAs.NOT_POSSIBLE);
    final ParameterExpression valueVariable;
    final Expression literalValue =
        appendConstant("literal_value", valueExpression);
    if (literalValue instanceof ParameterExpression) {
      valueVariable = (ParameterExpression) literalValue;
    } else {
      valueVariable =
          Expressions.parameter(valueExpression.getType(),
              list.newName("literal_value"));
      list.add(
          Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    }

    // Generate one line of code to check whether RexLiteral is null, e.g.,
    // "final boolean literal_isNull = false;"
    final Expression isNullExpression =
        literal.isNull() ? RexImpTable.TRUE_EXPR : RexImpTable.FALSE_EXPR;
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE, list.newName("literal_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    // Maintain the map from valueVariable (ParameterExpression) to real Expression
    literalMap.put(valueVariable, valueExpression);
    final Result result = new Result(isNullVariable, valueVariable);
    // Cache RexLiteral's result
    rexResultMap.put(literal, result);
    return result;
  }

  /**
   * Returns an {@code Expression} for null literal without losing its type
   * information.
   */
  private ConstantExpression getTypedNullLiteral(RexLiteral literal) {
    assert literal.isNull();
    Type javaClass = typeFactory.getJavaClass(literal.getType());
    switch (literal.getType().getSqlTypeName()) {
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      javaClass = Integer.class;
      break;
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      javaClass = Long.class;
      break;
    default:
      break;
    }
    return javaClass == null || javaClass == Void.class
        ? RexImpTable.NULL_EXPR
        : Expressions.constant(null, javaClass);
  }

  /**
   * Visit {@code RexCall}. For most {@code SqlOperator}s, we can get the implementor
   * from {@code RexImpTable}. Several operators (e.g., CaseWhen) with special semantics
   * need to be implemented separately.
   */
  @Override public Result visitCall(RexCall call) {
    if (rexResultMap.containsKey(call)) {
      return rexResultMap.get(call);
    }
    final SqlOperator operator = call.getOperator();
    /*if (operator == PREV) {
      return implementPrev(call);
    }*/
    if (operator == CASE) {
      return implementCaseWhen(call);
    }
    if (operator == SEARCH) {
      return RexUtil.expandSearch(builder, program, call).accept(this);
    }
    final RexImpTable.RexCallImplementor implementor =
        RexImpTable.INSTANCE.get(operator);
    if (implementor == null) {
      throw new RuntimeException("cannot translate call " + call);
    }
    final List<RexNode> operandList = call.getOperands();
    final List<@Nullable Type> storageTypes = ConverterUtils.internalTypes(operandList);
    final List<Result> operandResults = new ArrayList<>();
    for (int i = 0; i < operandList.size(); i++) {
      final Result operandResult =
          implementCallOperand(operandList.get(i), storageTypes.get(i), this);
      operandResults.add(operandResult);
    }
    callOperandResultMap.put(call, operandResults);
    final Result result = implementor.implement(this, call, operandResults);
    rexResultMap.put(call, result);
    return result;
  }

  private static Result implementCallOperand(final RexNode operand,
      final @Nullable Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    Result operandResult = operand.accept(translator);
    if (storageType != null) {
      operandResult = translator.toInnerStorageType(operandResult, storageType);
    }
    translator.currentStorageType = originalStorageType;
    return operandResult;
  }

  private static Expression implementCallOperand2(final RexNode operand,
      final @Nullable Type storageType, final RexToLixTranslator translator) {
    final Type originalStorageType = translator.currentStorageType;
    translator.currentStorageType = storageType;
    final Expression result =  translator.translate(operand);
    translator.currentStorageType = originalStorageType;
    return result;
  }

  /**
   * For {@code PREV} operator, the offset of {@code inputGetter}
   * should be set first.
   */
/*  private Result implementPrev(RexCall call) {
    final RexNode node = call.getOperands().get(0);
    final RexNode offset = call.getOperands().get(1);
    final Expression offs =
        Expressions.multiply(translate(offset), Expressions.constant(-1));
    requireNonNull((EnumerableMatch.PrevInputGetter) inputGetter, "inputGetter")
        .setOffset(offs);
    return node.accept(this);
  }*/

  /**
   * The CASE operator is SQL’s way of handling if/then logic.
   * Different with other {@code RexCall}s, it is not safe to
   * implement its operands first.
   * For example: {@code
   *   select case when s=0 then false
   *          else 100/s > 0 end
   *   from (values (1),(0)) ax(s);
   * }
   */
  private Result implementCaseWhen(RexCall call) {
    final Type returnType = typeFactory.getJavaClass(call.getType());
    final ParameterExpression valueVariable =
        Expressions.parameter(returnType,
            list.newName("case_when_value"));
    list.add(Expressions.declare(0, valueVariable, null));
    final List<RexNode> operandList = call.getOperands();
    implementRecursively(this, operandList, valueVariable, call.getType(), 0);
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(
            Boolean.TYPE, list.newName("case_when_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
    final Result result = new Result(isNullVariable, valueVariable);
    rexResultMap.put(call, result);
    return result;
  }

  /**
   * Case statements of the form:
   * {@code CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END}.
   * When {@code a = true}, returns {@code b};
   * when {@code c = true}, returns {@code d};
   * else returns {@code e}.
   *
   * <p>We generate code that looks like:
   *
   * <blockquote><pre>{@code
   *      int case_when_value;
   *      ......code for a......
   *      if (!a_isNull && a_value) {
   *          ......code for b......
   *          case_when_value = res(b_isNull, b_value);
   *      } else {
   *          ......code for c......
   *          if (!c_isNull && c_value) {
   *              ......code for d......
   *              case_when_value = res(d_isNull, d_value);
   *          } else {
   *              ......code for e......
   *              case_when_value = res(e_isNull, e_value);
   *          }
   *      }
   * }</pre></blockquote>
   */
  private static void implementRecursively(RexToLixTranslator currentTranslator,
      List<RexNode> operandList, ParameterExpression valueVariable, RelDataType valueType, int pos) {
    final BlockBuilder currentBlockBuilder =
        currentTranslator.getBlockBuilder();
    final List<@Nullable Type> storageTypes =
            ConverterUtils.internalTypes(operandList);
    // [ELSE] clause
    if (pos == operandList.size() - 1) {
      Expression res =
          implementCallOperand2(operandList.get(pos), storageTypes.get(pos),
              currentTranslator);
      currentBlockBuilder.add(
          Expressions.statement(
              Expressions.assign(valueVariable,
                      ConverterUtils.convert(res, valueType))));
      return;
    }
    // Condition code: !a_isNull && a_value
    final RexNode testerNode = operandList.get(pos);
    final Result testerResult =
        implementCallOperand(testerNode, storageTypes.get(pos),
            currentTranslator);
    final Expression tester =
        Expressions.andAlso(Expressions.not(testerResult.isNullVariable),
            testerResult.valueVariable);
    // Code for {if} branch
    final RexNode ifTrueNode = operandList.get(pos + 1);
    final BlockBuilder ifTrueBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifTrueTranslator =
        currentTranslator.setBlock(ifTrueBlockBuilder);
    final Expression ifTrueRes =
        implementCallOperand2(ifTrueNode, storageTypes.get(pos + 1),
            ifTrueTranslator);
    // Assign the value: case_when_value = ifTrueRes
    ifTrueBlockBuilder.add(
        Expressions.statement(
            Expressions.assign(valueVariable,
                    ConverterUtils.convert(ifTrueRes, valueType))));
    final BlockStatement ifTrue = ifTrueBlockBuilder.toBlock();
    // There is no [ELSE] clause
    if (pos + 1 == operandList.size() - 1) {
      currentBlockBuilder.add(
          Expressions.ifThen(tester, ifTrue));
      return;
    }
    // Generate code for {else} branch recursively
    final BlockBuilder ifFalseBlockBuilder =
        new BlockBuilder(true, currentBlockBuilder);
    final RexToLixTranslator ifFalseTranslator =
        currentTranslator.setBlock(ifFalseBlockBuilder);
    implementRecursively(ifFalseTranslator, operandList, valueVariable, valueType, pos + 2);
    final BlockStatement ifFalse = ifFalseBlockBuilder.toBlock();
    currentBlockBuilder.add(
        Expressions.ifThenElse(tester, ifTrue, ifFalse));
  }

  private Result toInnerStorageType(Result result, Type storageType) {
    final Expression valueExpression =
            ConverterUtils.toInternal(result.valueVariable, storageType);
    if (valueExpression.equals(result.valueVariable)) {
      return result;
    }
    final ParameterExpression valueVariable =
        Expressions.parameter(
            valueExpression.getType(),
            list.newName(result.valueVariable.name + "_inner_type"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable = result.isNullVariable;
    return new Result(isNullVariable, valueVariable);
  }

  @Override public Result visitDynamicParam(RexDynamicParam dynamicParam) {
    final Pair<RexNode, @Nullable Type> key =
        Pair.of(dynamicParam, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    // Calcite implementation is not applicable
    // SELECT CAST(? AS DECIMAL(1, 2)) with dyn param: new BigDecimal("0.12"), need to fail
/*    final Type storageType = currentStorageType != null
        ? currentStorageType : typeFactory.getJavaClass(dynamicParam.getType());

    final boolean isNumeric = SqlTypeFamily.NUMERIC.contains(dynamicParam.getType());

    // For numeric types, use java.lang.Number to prevent cast exception
    // when the parameter type differs from the target type

    final Expression valueExpression = isNumeric
        ? EnumUtils.convert(
            EnumUtils.convert(
                Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
                    Expressions.constant("?" + dynamicParam.getIndex())),
                java.lang.Number.class),
            storageType)
        : EnumUtils.convert(
            Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant("?" + dynamicParam.getIndex())),
            storageType);*/
    final Expression ctxGet = Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET.method,
            Expressions.constant("?" + dynamicParam.getIndex()));
    final Expression valueExpression =  ConverterUtils.convert(ctxGet, dynamicParam.getType());

    final ParameterExpression valueVariable =
        Expressions.parameter(valueExpression.getType(),
            list.newName("value_dynamic_param"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, valueExpression));
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE, list.newName("isNull_dynamic_param"));
    list.add(
        Expressions.declare(Modifier.FINAL, isNullVariable,
            checkNull(valueVariable)));
    final Result result = new Result(isNullVariable, valueVariable);
    rexWithStorageTypeResultMap.put(key, result);
    return result;
  }

  @Override public Result visitFieldAccess(RexFieldAccess fieldAccess) {
    final Pair<RexNode, @Nullable Type> key =
        Pair.of(fieldAccess, currentStorageType);
    if (rexWithStorageTypeResultMap.containsKey(key)) {
      return rexWithStorageTypeResultMap.get(key);
    }
    final RexNode target = deref(fieldAccess.getReferenceExpr());
    int fieldIndex = fieldAccess.getField().getIndex();
    String fieldName = fieldAccess.getField().getName();
    switch (target.getKind()) {
    case CORREL_VARIABLE:
      if (correlates == null) {
        throw new RuntimeException("Cannot translate " + fieldAccess
            + " since correlate variables resolver is not defined");
      }
      final RexToLixTranslator.InputGetter getter =
          correlates.apply(((RexCorrelVariable) target).getName());
      final Expression input =
          getter.field(list, fieldIndex, currentStorageType);
      final Expression condition = checkNull(input);
      final ParameterExpression valueVariable =
          Expressions.parameter(input.getType(), list.newName("corInp_value"));
      list.add(Expressions.declare(Modifier.FINAL, valueVariable, input));
      final ParameterExpression isNullVariable =
          Expressions.parameter(Boolean.TYPE, list.newName("corInp_isNull"));
      final Expression isNullExpression =
          Expressions.condition(condition,
              RexImpTable.TRUE_EXPR,
              checkNull(valueVariable));
      list.add(
          Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));
      final Result result1 = new Result(isNullVariable, valueVariable);
      rexWithStorageTypeResultMap.put(key, result1);
      return result1;
    default:
      RexNode rxIndex =
          builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
      RexNode rxName =
          builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
      RexCall accessCall =
          (RexCall) builder.makeCall(fieldAccess.getType(),
              SqlStdOperatorTable.STRUCT_ACCESS,
              ImmutableList.of(target, rxIndex, rxName));
      final Result result2 = accessCall.accept(this);
      rexWithStorageTypeResultMap.put(key, result2);
      return result2;
    }
  }

  @Override public Result visitOver(RexOver over) {
    throw new RuntimeException("cannot translate expression " + over);
  }

  @Override public Result visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new RuntimeException("Cannot translate " + correlVariable
        + ". Correlated variables should always be referenced by field access");
  }

  @Override public Result visitRangeRef(RexRangeRef rangeRef) {
    throw new RuntimeException("cannot translate expression " + rangeRef);
  }

  @Override public Result visitSubQuery(RexSubQuery subQuery) {
    throw new RuntimeException("cannot translate expression " + subQuery);
  }

  @Override public Result visitTableInputRef(RexTableInputRef fieldRef) {
    throw new RuntimeException("cannot translate expression " + fieldRef);
  }

  @Override public Result visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return visitInputRef(fieldRef);
  }

  @Override public Result visitLambda(RexLambda lambda) {
    final RexNode expression = lambda.getExpression();
    final List<RexLambdaRef> rexLambdaRefs = lambda.getParameters();

    // Prepare parameter expressions for lambda expression
    final ParameterExpression[] parameterExpressions =
        new ParameterExpression[rexLambdaRefs.size()];
    for (int i = 0; i < rexLambdaRefs.size(); i++) {
      final RexLambdaRef rexLambdaRef = rexLambdaRefs.get(i);
      parameterExpressions[i] =
          Expressions.parameter(
              typeFactory.getJavaClass(rexLambdaRef.getType()), rexLambdaRef.getName());
    }

    // Generate code for lambda expression body
    final RexToLixTranslator exprTranslator = this.setBlock(new BlockBuilder());
    final Result exprResult = expression.accept(exprTranslator);
    exprTranslator.list.add(
        Expressions.return_(null, exprResult.valueVariable));

    // Generate code for lambda expression
    final Expression functionExpression =
        Expressions.lambda(exprTranslator.list.toBlock(), parameterExpressions);
    final ParameterExpression valueVariable =
        Expressions.parameter(functionExpression.getType(), list.newName("function_value"));
    list.add(Expressions.declare(Modifier.FINAL, valueVariable, functionExpression));

    // Generate code for checking whether lambda expression is null
    final Expression isNullExpression = checkNull(valueVariable);
    final ParameterExpression isNullVariable =
        Expressions.parameter(Boolean.TYPE, list.newName("function_isNull"));
    list.add(Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression));

    return new Result(isNullVariable, valueVariable);
  }

  Expression checkNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.FALSE_EXPR;
    }
    return Expressions.equal(expr, RexImpTable.NULL_EXPR);
  }

  Expression checkNotNull(Expression expr) {
    if (Primitive.flavor(expr.getType())
        == Primitive.Flavor.PRIMITIVE) {
      return RexImpTable.TRUE_EXPR;
    }
    return Expressions.notEqual(expr, RexImpTable.NULL_EXPR);
  }

  BlockBuilder getBlockBuilder() {
    return list;
  }

  Expression getLiteral(Expression literalVariable) {
    return requireNonNull(literalMap.get(literalVariable),
        () -> "literalMap.get(literalVariable) for " + literalVariable);
  }

  /** Returns the value of a literal. */
  @Nullable Object getLiteralValue(@Nullable Expression expr) {
    if (expr instanceof ParameterExpression) {
      final Expression constantExpr = literalMap.get(expr);
      return getLiteralValue(constantExpr);
    }
    if (expr instanceof ConstantExpression) {
      return ((ConstantExpression) expr).value;
    }
    return null;
  }

  List<Result> getCallOperandResult(RexCall call) {
    return requireNonNull(callOperandResultMap.get(call),
        () -> "callOperandResultMap.get(call) for " + call);
  }

  /** Returns an expression that yields the function object whose method
   * we are about to call.
   *
   * <p>It might be 'new MyFunction()', but it also might be a reference
   * to a static field 'F', defined by
   * 'static final MyFunction F = new MyFunction()'.
   *
   * <p>If there is a constructor that takes a {@link FunctionContext}
   * argument, we call that, passing in the values of arguments that are
   * literals; this allows the function to do some computation at load time.
   *
   * <p>If the call is "f(1, 2 + 3, 'foo')" and "f" is implemented by method
   * "eval(int, int, String)" in "class MyFun", the expression might be
   * "new MyFunction(FunctionContexts.of(new Object[] {1, null, "foo"})".
   *
   * @param method Method that implements the UDF
   * @param call Call to the UDF
   * @return New expression
   */
  Expression functionInstance(RexCall call, Method method) {
    final RexCallBinding callBinding =
        RexCallBinding.create(typeFactory, call, program, ImmutableList.of());
    final Expression target = getInstantiationExpression(method, callBinding);
    return appendConstant("f", target);
  }

  /** Helper for {@link #functionInstance}. */
  private Expression getInstantiationExpression(Method method,
      RexCallBinding callBinding) {
    final Class<?> declaringClass = method.getDeclaringClass();
    // If the UDF class has a constructor that takes a Context argument,
    // use that.
    try {
      final Constructor<?> constructor =
          declaringClass.getConstructor(FunctionContext.class);
      final List<Expression> constantArgs = new ArrayList<>();
      //noinspection unchecked
      Ord.forEach(method.getParameterTypes(),
          (parameterType, i) ->
              constantArgs.add(
                  callBinding.isOperandLiteral(i, true)
                      ? appendConstant("_arg",
                      Expressions.constant(
                          callBinding.getOperandLiteralValue(i,
                              Primitive.box(parameterType))))
                      : Expressions.constant(null)));
      final Expression context =
          Expressions.call(BuiltInMethod.FUNCTION_CONTEXTS_OF.method,
              DataContext.ROOT,
              Expressions.newArrayInit(Object.class, constantArgs));
      return Expressions.new_(constructor, context);
    } catch (NoSuchMethodException e) {
      // ignore
    }
    // The UDF class must have a public zero-args constructor.
    // Assume that the validator checked already.
    return Expressions.new_(declaringClass);
  }

  /** Stores a constant expression in a variable. */
  private Expression appendConstant(String name, Expression e) {
    if (staticList != null) {
      // If name is "camelCase", upperName is "CAMEL_CASE".
      final String upperName =
          CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, name);
      return staticList.append(upperName, e);
    } else {
      return list.append(name, e);
    }
  }

  /** Translates a field of an input to an expression. */
  public interface InputGetter {
    Expression field(BlockBuilder list, int index, @Nullable Type storageType);
  }

  /** Implementation of {@link InputGetter} that calls
   * {@link PhysType#fieldReference}. */
  public static class InputGetterImpl implements InputGetter {
    private final ImmutableMap<Expression, PhysType> inputs;

    @Deprecated // to be removed before 2.0
    public InputGetterImpl(List<Pair<Expression, PhysType>> inputs) {
      this(mapOf(inputs));
    }

    public InputGetterImpl(Expression e, PhysType physType) {
      this(ImmutableMap.of(e, physType));
    }

    public InputGetterImpl(Map<Expression, PhysType> inputs) {
      this.inputs = ImmutableMap.copyOf(inputs);
    }

    private static <K, V> Map<K, V> mapOf(
        Iterable<? extends Map.Entry<K, V>> entries) {
      ImmutableMap.Builder<K, V> b = ImmutableMap.builder();
      Pair.forEach(entries, b::put);
      return b.build();
    }

    @Override public Expression field(BlockBuilder list, int index, @Nullable Type storageType) {
      int offset = 0;
      for (Map.Entry<Expression, PhysType> input : inputs.entrySet()) {
        final PhysType physType = input.getValue();
        int fieldCount = physType.getRowType().getFieldCount();
        if (index >= offset + fieldCount) {
          offset += fieldCount;
          continue;
        }
        final Expression left = list.append("current", input.getKey());
        return physType.fieldReference(left, index - offset, storageType);
      }
      throw new IllegalArgumentException("Unable to find field #" + index);
    }
  }

  /** Result of translating a {@code RexNode}. */
  public static class Result {
    final ParameterExpression isNullVariable;
    final ParameterExpression valueVariable;

    public Result(ParameterExpression isNullVariable,
        ParameterExpression valueVariable) {
      this.isNullVariable = isNullVariable;
      this.valueVariable = valueVariable;
    }
  }
}
//CHECKSTYLE:ON
