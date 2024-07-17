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

namespace Apache.Ignite.Internal.Linq;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using Common;
using NodaTime;

/// <summary>
/// MethodCall expression visitor. Maps CLR methods to SQL functions.
/// <para />
/// Refer to https://calcite.apache.org/docs/reference.html for supported SQL functions.
/// </summary>
internal static class MethodVisitor
{
    private const string TrimBoth = "both";

    private const string TrimLeading = "leading";

    private const string TrimTrailing = "trailing";

    /// <summary> Property visitors. </summary>
    private static readonly Dictionary<MemberInfo, string> Properties = new()
    {
        {typeof(string).GetProperty(nameof(string.Length))!, "length"},
        {typeof(LocalDate).GetProperty(nameof(LocalDate.Year))!, "year"},
        {typeof(LocalDate).GetProperty(nameof(LocalDate.Month))!, "month"},
        {typeof(LocalDate).GetProperty(nameof(LocalDate.Day))!, "dayofmonth"},
        {typeof(LocalDate).GetProperty(nameof(LocalDate.DayOfYear))!, "dayofyear"},
        {typeof(LocalDate).GetProperty(nameof(LocalDate.DayOfWeek))!, "-1 + dayofweek"},
        {typeof(LocalTime).GetProperty(nameof(LocalTime.Hour))!, "hour"},
        {typeof(LocalTime).GetProperty(nameof(LocalTime.Minute))!, "minute"},
        {typeof(LocalTime).GetProperty(nameof(LocalTime.Second))!, "second"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.Year))!, "year"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.Month))!, "month"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.Day))!, "dayofmonth"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.DayOfYear))!, "dayofyear"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.DayOfWeek))!, "-1 + dayofweek"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.Hour))!, "hour"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.Minute))!, "minute"},
        {typeof(LocalDateTime).GetProperty(nameof(LocalDateTime.Second))!, "second"}
    };

    /// <summary>
    /// Delegates dictionary.
    /// </summary>
    private static readonly Dictionary<MethodInfo, VisitMethodDelegate> Delegates = new List
            <KeyValuePair<MethodInfo?, VisitMethodDelegate>>
            {
                GetStringMethod(nameof(string.ToLower), Type.EmptyTypes, GetFunc("lower")),
                GetStringMethod(nameof(string.ToUpper), Type.EmptyTypes, GetFunc("upper")),
                GetStringMethod(nameof(string.Contains), new[] {typeof(string)}, (e, v) => VisitSqlLike(e, v, "'%' || ? || '%'")),
                GetStringMethod(nameof(string.StartsWith), new[] {typeof(string)}, (e, v) => VisitSqlLike(e, v, "? || '%'")),
                GetStringMethod(nameof(string.EndsWith), new[] {typeof(string)}, (e, v) => VisitSqlLike(e, v, "'%' || ?")),
                GetStringMethod(nameof(string.IndexOf), new[] {typeof(string)}, VisitPositionFunc),
                GetStringMethod(nameof(string.IndexOf), new[] {typeof(string), typeof(int)}, VisitPositionFunc),
                GetStringMethod(nameof(string.Substring), new[] {typeof(int)}, GetFunc("substring", 0, 1)),
                GetStringMethod(nameof(string.Substring), new[] {typeof(int), typeof(int)}, GetFunc("substring", 0, 1)),
                GetStringMethod(nameof(string.Trim), "trim"),
                GetStringMethod(nameof(string.TrimStart), "ltrim"),
                GetStringMethod(nameof(string.TrimEnd), "rtrim"),
                GetParameterizedTrimMethod(nameof(string.Trim), TrimBoth),
                GetParameterizedTrimMethod(nameof(string.TrimStart), TrimLeading),
                GetParameterizedTrimMethod(nameof(string.TrimEnd), TrimTrailing),
                GetCharTrimMethod(nameof(string.Trim), TrimBoth),
                GetCharTrimMethod(nameof(string.TrimStart), TrimLeading),
                GetCharTrimMethod(nameof(string.TrimEnd), TrimTrailing),
                GetStringMethod(nameof(string.Replace), "replace", typeof(string), typeof(string)),
                GetStringMethod(nameof(string.Compare), new[] { typeof(string), typeof(string) }, (e, v) => VisitStringCompare(e, v, false)),
                GetStringMethod(nameof(string.Compare), new[] { typeof(string), typeof(string), typeof(bool) }, (e, v) => VisitStringCompare(e, v, GetStringCompareIgnoreCaseParameter(e.Arguments[2]))),

                GetRegexMethod(nameof(Regex.Replace), "regexp_replace", typeof(string), typeof(string), typeof(string)),
                GetRegexMethod(nameof(Regex.Replace), "regexp_replace", typeof(string), typeof(string), typeof(string), typeof(RegexOptions)),
                GetRegexMethod(nameof(Regex.IsMatch), "regexp_like", typeof(string), typeof(string)),
                GetRegexMethod(nameof(Regex.IsMatch), "regexp_like", typeof(string), typeof(string), typeof(RegexOptions)),

                GetMethod(typeof(DateTime), "ToString", new[] {typeof(string)}, (e, v) => VisitFunc(e, v, "formatdatetime", ", 'en', 'UTC'")),

                GetMathMethod(nameof(Math.Abs), typeof(int)),
                GetMathMethod(nameof(Math.Abs), typeof(long)),
                GetMathMethod(nameof(Math.Abs), typeof(float)),
                GetMathMethod(nameof(Math.Abs), typeof(double)),
                GetMathMethod(nameof(Math.Abs), typeof(decimal)),
                GetMathMethod(nameof(Math.Abs), typeof(sbyte)),
                GetMathMethod(nameof(Math.Abs), typeof(short)),
                GetMathMethod(nameof(Math.Acos), typeof(double)),
                GetMathMethod(nameof(Math.Acosh), typeof(double)),
                GetMathMethod(nameof(Math.Asin), typeof(double)),
                GetMathMethod(nameof(Math.Asinh), typeof(double)),
                GetMathMethod(nameof(Math.Atan), typeof(double)),
                GetMathMethod(nameof(Math.Atanh), typeof(double)),
                GetMathMethod(nameof(Math.Atan2), typeof(double), typeof(double)),
                GetMathMethod(nameof(Math.Ceiling), typeof(double)),
                GetMathMethod(nameof(Math.Ceiling), typeof(decimal)),
                GetMathMethod(nameof(Math.Cos), typeof(double)),
                GetMathMethod(nameof(Math.Cosh), typeof(double)),
                GetMathMethod(nameof(Math.Exp), typeof(double)),
                GetMathMethod(nameof(Math.Floor), typeof(double)),
                GetMathMethod(nameof(Math.Floor), typeof(decimal)),
                GetMathMethod(nameof(Math.Log), "Ln", typeof(double)),
                GetMathMethod(nameof(Math.Log10), typeof(double)),
                GetMathMethod(nameof(Math.Log2), typeof(double)),
                GetMathMethod(nameof(Math.Pow), "Power", typeof(double), typeof(double)),
                GetMathMethod(nameof(Math.Round), typeof(double)),
                GetMathMethod(nameof(Math.Round), typeof(double), typeof(int)),
                GetMathMethod(nameof(Math.Round), typeof(decimal)),
                GetMathMethod(nameof(Math.Round), typeof(decimal), typeof(int)),
                GetMathMethod(nameof(Math.Sign), typeof(double)),
                GetMathMethod(nameof(Math.Sign), typeof(decimal)),
                GetMathMethod(nameof(Math.Sign), typeof(float)),
                GetMathMethod(nameof(Math.Sign), typeof(int)),
                GetMathMethod(nameof(Math.Sign), typeof(long)),
                GetMathMethod(nameof(Math.Sign), typeof(short)),
                GetMathMethod(nameof(Math.Sign), typeof(sbyte)),
                GetMathMethod(nameof(Math.Sin), typeof(double)),
                GetMathMethod(nameof(Math.Sinh), typeof(double)),
                GetMathMethod(nameof(Math.Sqrt), typeof(double)),
                GetMathMethod(nameof(Math.Tan), typeof(double)),
                GetMathMethod(nameof(Math.Tanh), typeof(double)),
                GetMathMethod(nameof(Math.Truncate), typeof(double)),
                GetMathMethod(nameof(Math.Truncate), typeof(decimal)),
            }
        .Where(x => x.Key != null)
        .ToDictionary(x => x.Key!, x => x.Value);

    /// <summary> RegexOptions transformations. </summary>
    private static readonly Dictionary<RegexOptions, string> RegexOptionFlags = new()
    {
        { RegexOptions.IgnoreCase, "i" },
        { RegexOptions.Multiline, "m" }
    };

    /// <summary> Method visit delegate. </summary>
    [SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix", Justification = "Private.")]
    private delegate void VisitMethodDelegate(MethodCallExpression expression, IgniteQueryExpressionVisitor visitor);

    /// <summary>
    /// Visits a property call expression.
    /// </summary>
    /// <param name="expression">Expression.</param>
    /// <param name="visitor">Visitor.</param>
    /// <returns>Success flag.</returns>
    public static bool VisitPropertyCall(MemberExpression expression, IgniteQueryExpressionVisitor visitor)
    {
        if (!Properties.TryGetValue(expression.Member, out var funcName) || expression.Expression == null)
        {
            return false;
        }

        visitor.ResultBuilder.Append(funcName).Append('(');

        visitor.Visit(expression.Expression);

        visitor.ResultBuilder.Append(')');

        return true;
    }

    /// <summary>
    /// Visits a method call expression.
    /// </summary>
    /// <param name="expression">Expression.</param>
    /// <param name="visitor">Visitor.</param>
    public static void VisitMethodCall(MethodCallExpression expression, IgniteQueryExpressionVisitor visitor)
    {
        var mtd = expression.Method;

        if (!Delegates.TryGetValue(mtd, out var del))
        {
            throw new NotSupportedException(string.Format(
                CultureInfo.InvariantCulture,
                "Method not supported: {0}.({1})",
                mtd.DeclaringType == null ? "static" : mtd.DeclaringType.FullName,
                mtd));
        }

        del(expression, visitor);
    }

    /// <summary>
    /// Visits a constant call expression.
    /// </summary>
    /// <param name="expression">Expression.</param>
    /// <param name="visitor">Visitor.</param>
    /// <returns>Success flag.</returns>
    public static bool VisitConstantCall(ConstantExpression expression, IgniteQueryExpressionVisitor visitor)
    {
        if (expression.Type != typeof(RegexOptions))
        {
            return false;
        }

        var regexOptions = expression.Value as RegexOptions? ?? RegexOptions.None;
        var result = string.Empty;
        foreach (var option in RegexOptionFlags)
        {
            if (regexOptions.HasFlag(option.Key))
            {
                result += option.Value;
                regexOptions &= ~option.Key;
            }
        }

        if (regexOptions != RegexOptions.None)
        {
            throw new NotSupportedException($"RegexOptions.{regexOptions} is not supported");
        }

        // "pos" and "occurence" are required before "matchType".
        visitor.ResultBuilder.Append("1, 1, ");

        visitor.AppendParameter(result);

        return true;
    }

    /// <summary>
    /// Gets the function.
    /// </summary>
    private static VisitMethodDelegate GetFunc(string func, params int[] adjust) =>
        (e, v) => VisitFunc(e, v, func, null, adjust);

    /// <summary>
    /// Visits the instance function.
    /// </summary>
    private static void VisitFunc(
        MethodCallExpression expression,
        IgniteQueryExpressionVisitor visitor,
        string func,
        string? suffix,
        params int[] adjust)
    {
        visitor.ResultBuilder.Append(func).Append('(');

        var isInstanceMethod = expression.Object != null;

        if (isInstanceMethod)
        {
            visitor.Visit(expression.Object!);
        }

        for (int i = 0; i < expression.Arguments.Count; i++)
        {
            var arg = expression.Arguments[i];

            if (isInstanceMethod || (i > 0))
            {
                visitor.ResultBuilder.Append(", ");
            }

            visitor.Visit(arg);

            AppendAdjustment(visitor, adjust, i + 1);
        }

        visitor.ResultBuilder.Append(suffix).Append(')');

        AppendAdjustment(visitor, adjust, 0);
    }

    /// <summary>
    /// Visits the instance function for Trim specific handling.
    /// </summary>
    private static void VisitParameterizedTrimFunc(
        MethodCallExpression expression,
        IgniteQueryExpressionVisitor visitor,
        string mode)
    {
        // trim(leading|trailing|both chars from string)
        visitor.ResultBuilder.Append("trim(").Append(mode).Append(' ');

        if (expression.Arguments.Count > 0 && expression.Arguments[0] is { } arg)
        {
            if (arg is ConstantExpression constant)
            {
                if (constant.Value is char ch)
                {
                    visitor.AppendParameter(ch);
                }
                else
                {
                    var args = constant.Value as IEnumerable<char>;

                    if (args == null)
                    {
                        throw new NotSupportedException("String.Trim function only supports IEnumerable<char>");
                    }

                    var enumeratedArgs = args.ToArray();

                    if (enumeratedArgs.Length != 1)
                    {
                        throw new NotSupportedException("String.Trim function only supports a single argument: " + expression);
                    }

                    visitor.AppendParameter(enumeratedArgs[0]);
                }
            }
            else
            {
                visitor.Visit(arg);
            }
        }

        visitor.ResultBuilder.TrimEnd().Append(" from ");
        visitor.Visit(expression.Object!);
        visitor.ResultBuilder.Append(')');
    }

    /// <summary>
    /// Visits the function for IndexOf -> POSITION mapping.
    /// </summary>
    private static void VisitPositionFunc(
        MethodCallExpression expression,
        IgniteQueryExpressionVisitor visitor)
    {
        // POSITION(string1 IN string2)
        // Returns 1-based index when substring is found, 0 when not found.
        visitor.ResultBuilder.Append("-1 + position(");

        Debug.Assert(expression.Arguments.Count >= 1, "expression.Arguments.Count >= 1");

        visitor.Visit(expression.Arguments[0]);
        visitor.ResultBuilder.TrimEnd().Append(" in ");
        visitor.Visit(expression.Object!);

        if (expression.Arguments.Count > 1)
        {
            // POSITION(string1 IN string2 FROM integer)
            visitor.ResultBuilder.TrimEnd().Append(" from (");
            visitor.Visit(expression.Arguments[1]);
            visitor.ResultBuilder.Append(" + 1)");
        }

        visitor.ResultBuilder.TrimEnd().Append(')');
    }

    /// <summary>
    /// Appends the adjustment.
    /// </summary>
    private static void AppendAdjustment(IgniteQueryExpressionVisitor visitor, int[] adjust, int idx)
    {
        if (idx < adjust.Length)
        {
            var delta = adjust[idx];

            if (delta > 0)
            {
                visitor.ResultBuilder.AppendFormat(CultureInfo.InvariantCulture,  " + {0}", delta);
            }
            else if (delta < 0)
            {
                visitor.ResultBuilder.AppendFormat(CultureInfo.InvariantCulture, " {0}", delta);
            }
        }
    }

    /// <summary>
    /// Visits the SQL like expression.
    /// </summary>
    private static void VisitSqlLike(
        MethodCallExpression expression,
        IgniteQueryExpressionVisitor visitor,
        string likeFormat)
    {
        visitor.ResultBuilder.Append('(');

        visitor.Visit(expression.Object!);

        visitor.ResultBuilder.AppendFormat(CultureInfo.InvariantCulture, " like {0}) ", likeFormat);

        var paramValue = expression.Arguments[0] is ConstantExpression arg
            ? arg.Value
            : ExpressionWalker.EvaluateExpression<object>(expression.Arguments[0]);

        visitor.Parameters.Add(paramValue);
    }

    /// <summary>
    /// Get IgnoreCase parameter for string.Compare method.
    /// </summary>
    private static bool GetStringCompareIgnoreCaseParameter(Expression expression)
    {
        if (expression is ConstantExpression { Value: bool } constant)
        {
            return (bool)constant.Value;
        }

        throw new NotSupportedException(
            "Parameter 'ignoreCase' from 'string.Compare method should be specified as a constant expression");
    }

    /// <summary>
    /// Visits string.Compare method.
    /// </summary>
    private static void VisitStringCompare(MethodCallExpression expression, IgniteQueryExpressionVisitor visitor, bool ignoreCase)
    {
        // case when (A is not distinct from B) then 0 else (case (A > B) when true then 1 else -1 end) end
        var builder = visitor.ResultBuilder;

        builder.Append("case when (");
        VisitArg(visitor, expression, 0, ignoreCase);
        builder.Append(" is not distinct from ");
        VisitArg(visitor, expression, 1, ignoreCase);
        builder.Append(") then 0 else (case when (");
        VisitArg(visitor, expression, 0, ignoreCase);
        builder.Append(" > ");
        VisitArg(visitor, expression, 1, ignoreCase);
        builder.Append(") then 1 else -1 end) end");
    }

    /// <summary>
    /// Visits member expression argument.
    /// </summary>
    private static void VisitArg(
        IgniteQueryExpressionVisitor visitor,
        MethodCallExpression expression,
        int idx,
        bool lower)
    {
        if (lower)
        {
            visitor.ResultBuilder.Append("lower(");
        }

        visitor.Visit(expression.Arguments[idx]);

        if (lower)
        {
            visitor.ResultBuilder.Append(')');
        }
    }

    /// <summary>
    /// Gets the method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetMethod(
        Type type,
        string name,
        Type[]? argTypes = null,
        VisitMethodDelegate? del = null)
    {
        var method = argTypes == null ? type.GetMethod(name) : type.GetMethod(name, argTypes);

        return new KeyValuePair<MethodInfo?, VisitMethodDelegate>(method!, del ?? GetFunc(name));
    }

    /// <summary>
    /// Gets the string method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetStringMethod(
        string name,
        Type[]? argTypes = null,
        VisitMethodDelegate? del = null)
    {
        return GetMethod(typeof(string), name, argTypes, del);
    }

    /// <summary>
    /// Gets the string method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetStringMethod(
        string name,
        string sqlName,
        params Type[] argTypes)
    {
        return GetMethod(typeof(string), name, argTypes, GetFunc(sqlName));
    }

    /// <summary>
    /// Gets the Regex method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetRegexMethod(
        string name,
        string sqlName,
        params Type[] argTypes)
    {
        return GetMethod(typeof(Regex), name, argTypes, GetFunc(sqlName));
    }

    /// <summary>
    /// Gets string parameterized Trim(TrimStart, TrimEnd) method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetParameterizedTrimMethod(string name, string mode) =>
        GetMethod(
            typeof(string),
            name,
            new[] {typeof(char[])},
            (e, v) => VisitParameterizedTrimFunc(e, v, mode));

    /// <summary>
    /// Gets string parameterized Trim(TrimStart, TrimEnd) method that takes a single char.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetCharTrimMethod(string name, string mode) =>
        GetMethod(
            typeof(string),
            name,
            new[] {typeof(char)},
            (e, v) => VisitParameterizedTrimFunc(e, v, mode));

    /// <summary>
    /// Gets the math method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetMathMethod(
        string name,
        string sqlName,
        params Type[] argTypes) =>
        GetMethod(typeof(Math), name, argTypes, GetFunc(sqlName));

    /// <summary>
    /// Gets the math method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetMathMethod(string name, params Type[] argTypes) =>
        GetMathMethod(name, name, argTypes);
}
