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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.ignite.internal.sql.engine.api.expressions.RowAccessor;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlEvaluationContext;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;

/**
 * Contains methods used in metadata definitions.
 */
public enum IgniteMethod {
    /** See {@link RowBuilder#addField(Object)} )}. */
    ROW_BUILDER_ADD_FIELD(RowBuilder.class, "addField", Object.class),

    /** See {@link RowHandler#get(int, Object)}. */
    ROW_ACCESSOR_GET(RowAccessor.class, "get", int.class, Object.class),

    /** See {@link SqlEvaluationContext#rowAccessor()}. */
    CONTEXT_ROW_HANDLER(SqlEvaluationContext.class, "rowAccessor"),

    /** See {@link SqlEvaluationContext#correlatedVariable(int)}. */
    CONTEXT_GET_CORRELATED_VALUE(SqlEvaluationContext.class, "correlatedVariable", int.class),

    /** See {@link IgniteSqlDateTimeUtils#subtractTimeZoneOffset(long, TimeZone)}. **/
    SUBTRACT_TIMEZONE_OFFSET(IgniteSqlDateTimeUtils.class, "subtractTimeZoneOffset", long.class, TimeZone.class),

    /** See {@link IgniteSqlFunctions#toDateExact(int)}. **/
    TO_DATE_EXACT(IgniteSqlFunctions.class, "toDateExact", int.class),

    /** See {@link IgniteSqlFunctions#toTimestampExact(Object)}. **/
    TO_TIMESTAMP_EXACT(IgniteSqlFunctions.class, "toTimestampExact", long.class),

    /** See {@link IgniteSqlFunctions#toTimestampLtzExact(Object)}. **/
    TO_TIMESTAMP_LTZ_EXACT(IgniteSqlFunctions.class, "toTimestampLtzExact", Object.class),

    /** See {@link SqlParserUtil#intervalToMonths(String, SqlIntervalQualifier)}. */
    PARSE_INTERVAL_YEAR_MONTH(SqlParserUtil.class, "intervalToMonths", String.class, SqlIntervalQualifier.class),

    /** See {@link SqlParserUtil#intervalToMillis(String, SqlIntervalQualifier)}. */
    PARSE_INTERVAL_DAY_TIME(SqlParserUtil.class, "intervalToMillis", String.class, SqlIntervalQualifier.class),

    /** See {@link IgniteSqlFunctions#toString(ByteString)}. */
    BYTESTRING_TO_STRING(IgniteSqlFunctions.class, "toString", ByteString.class),

    /** See {@link IgniteSqlFunctions#toByteString(String)}. */
    STRING_TO_BYTESTRING(IgniteSqlFunctions.class, "toByteString", String.class),

    /** See {@link IgniteSqlFunctions#least2(Object, Object)}. */
    LEAST2(IgniteSqlFunctions.class, "least2", Object.class, Object.class),

    /** See {@link IgniteSqlFunctions#greatest2(Object, Object)}. */
    GREATEST2(IgniteSqlFunctions.class, "greatest2", Object.class, Object.class),

    /** See {@link Objects#equals(Object, Object)}. */
    IS_NOT_DISTINCT_FROM(Objects.class, "equals", Object.class, Object.class),

    /** See {@link UUID#randomUUID()}. */
    RAND_UUID(IgniteSqlFunctions.class, "randUuid"),

    LENGTH(IgniteSqlFunctions.class, "length", Object.class),

    OCTET_LENGTH(IgniteSqlFunctions.class, "octetLength", ByteString.class),
    OCTET_LENGTH2(IgniteSqlFunctions.class, "octetLength", String.class),

    /** See {@link IgniteSqlFunctions#consumeFirstArgument(Object, Object)}. **/
    CONSUME_FIRST_ARGUMENT(IgniteSqlFunctions.class, "consumeFirstArgument", Object.class, Object.class),

    SUBSTR(SqlFunctions.class, "substring", String.class, int.class, int.class),

    /** ROUND function. See {@link IgniteSqlFunctions#sround(double)}, {@link IgniteSqlFunctions#sround(double, int)} and variants. */
    ROUND(IgniteSqlFunctions.class, "sround", true),

    /**
     * TRUNCATE function. See {@link IgniteSqlFunctions#struncate(double)}, {@link IgniteSqlFunctions#struncate(double, int)} and variants.
     */
    TRUNCATE(IgniteSqlFunctions.class, "struncate", true),

    LN(IgniteSqlFunctions.class, "log", true),
    LOG(IgniteSqlFunctions.class, "log", true),
    LOG10(IgniteSqlFunctions.class, "log10", true),

    /**
     * Decimal division as well as division operator used by REDUCE phase of AVG aggregate.
     * See {@link IgniteMath#decimalDivide(BigDecimal, BigDecimal, int, int)}.
     */
    DECIMAL_DIVIDE(IgniteMath.class, "decimalDivide", BigDecimal.class, BigDecimal.class, int.class, int.class),

    /**
     * Conversion of timestamp to string (precision aware).
     * See {@link IgniteSqlDateTimeUtils#unixTimestampToString(long, int)}.
     */
    UNIX_TIMESTAMP_TO_STRING_PRECISION_AWARE(IgniteSqlDateTimeUtils.class, "unixTimestampToString", long.class, int.class),

    /**
     * Conversion of time to string (precision aware).
     * See {@link IgniteSqlDateTimeUtils#unixTimeToString(int, int)}.
     */
    UNIX_TIME_TO_STRING_PRECISION_AWARE(IgniteSqlDateTimeUtils.class, "unixTimeToString", int.class, int.class),

    /**
     * Converts a timestamp string to a unix date (unlike the original version, truncation to milliseconds occurs without rounding).
     * See {@link IgniteSqlDateTimeUtils#timestampStringToUnixDate(String)}.
     */
    STRING_TO_TIMESTAMP(IgniteSqlDateTimeUtils.class, "timestampStringToUnixDate", String.class),

    /**
     * Converts a time string to a unix time (unlike the original version, truncation to milliseconds occurs without rounding).
     * See {@link IgniteSqlDateTimeUtils#timeStringToUnixDate(String)}.
     */
    STRING_TO_TIME(IgniteSqlDateTimeUtils.class, "timeStringToUnixDate", String.class),

    /**
     * SQL {@code CURRENT_DATE} function.
     * See {@link IgniteSqlDateTimeUtils#currentDate(DataContext)}.
     */
    CURRENT_DATE(IgniteSqlDateTimeUtils.class, "currentDate", DataContext.class),

    /**
     * SQL CAST({@code varchar} AS TIME FORMAT {@code format}).
     */
    TIME_STRING_TO_TIME(IgniteSqlFunctions.class, "toTime", String.class, String.class),

    /**
     * SQL CAST({@code varchar} AS DATE FORMAT {@code format}).
     */
    DATE_STRING_TO_DATE(IgniteSqlFunctions.class, "toDate", String.class, String.class),

    /**
     * SQL CAST({@code varchar} AS TIMESTAMP FORMAT {@code format}).
     */
    TIMESTAMP_STRING_TO_TIMESTAMP(IgniteSqlFunctions.class, "toTimestamp", String.class, String.class),

    /**
     * SQL CAST({@code varchar} AS TIMESTAMP WITH LOCAL TIME ZONE FORMAT {@code format}). The same as
     * {@link SqlFunctions#timeWithLocalTimeZoneToTimestampWithLocalTimeZone} but accepts date format literal.
     */
    TIMESTAMP_STRING_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE(IgniteSqlFunctions.class,
            "toTimestampWithLocalTimeZone", String.class, String.class, TimeZone.class),

    /**
     * SQL CAST({@code TIME} AS VARCHAR FORMAT {@code format}).
     */
    FORMAT_TIME(IgniteSqlFunctions.class, "formatTime", String.class, Integer.class),

    /**
     * SQL CAST({@code DATE} AS VARCHAR FORMAT {@code format}).
     */
    FORMAT_DATE(IgniteSqlFunctions.class, "formatDate", String.class, Integer.class),

    /**
     * SQL CAST({@code TIMESTAMP} AS VARCHAR FORMAT {@code format}).
     */
    FORMAT_TIMESTAMP(IgniteSqlFunctions.class, "formatTimestamp", String.class, Long.class),

    /**
     * SQL CAST({@code TIMESTAMP WITH LOCAL TIME ZONE} AS VARCHAR FORMAT {@code format}).
     */
    FORMAT_TIMESTAMP_WITH_LOCAL_TIME_ZONE(IgniteSqlFunctions.class, "formatTimestampWithLocalTimeZone",
            String.class, Long.class, TimeZone.class),

    /**
     * Returns the timestamp value truncated to the specified fraction of a second.
     * See {@link IgniteSqlDateTimeUtils#adjustTimestampMillis(Long, int)}.
     */
    ADJUST_TIMESTAMP_MILLIS(IgniteSqlDateTimeUtils.class, "adjustTimestampMillis", Long.class, int.class),

    /**
     * Returns the time value truncated to the specified fraction of a second.
     * See {@link IgniteSqlDateTimeUtils#adjustTimeMillis(Integer, int)}.
     */
    ADJUST_TIME_MILLIS(IgniteSqlDateTimeUtils.class, "adjustTimeMillis", Integer.class, int.class),

    /** See {@link IgniteSqlFunctions#findPrefix(String, String)}. */
    FIND_PREFIX(IgniteSqlFunctions.class, "findPrefix", String.class, String.class),

    /** See {@link IgniteSqlFunctions#nextGreaterPrefix(String)}. */
    NEXT_GREATER_PREFIX(IgniteSqlFunctions.class, "nextGreaterPrefix", String.class),

    ;

    private final Method method;

    /**
     * Constructor.
     *
     * @param clazz         Class where to lookup method.
     * @param methodName    Method name.
     * @param argumentTypes Method parameters types.
     */
    IgniteMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }

    /**
     * Constructor that allows to specify overloaded methods as SQL function implementations.
     *
     * @param clazz Class where to lookup method.
     * @param methodName Method name.
     * @param overloadedMethod If {@code true} to looks up overloaded methods, otherwise looks up a method w/o parameters.
     */
    IgniteMethod(Class<?> clazz, String methodName, boolean overloadedMethod) {
        if (overloadedMethod) {
            // Allow calcite to select appropriate method at a call site.
            this.method = Arrays.stream(clazz.getMethods())
                    .filter(m -> m.getName().equals(methodName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(format("Public static method {} is not defined", methodName)));
        } else {
            this.method = Types.lookupMethod(clazz, methodName);
        }
    }

    /**
     * Get method.
     */
    public Method method() {
        return method;
    }
}
