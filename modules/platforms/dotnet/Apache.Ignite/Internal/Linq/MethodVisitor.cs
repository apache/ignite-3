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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

/// <summary>
/// MethodCall expression visitor.
/// </summary>
internal static class MethodVisitor
{
    /// <summary> Property visitors. </summary>
    private static readonly Dictionary<MemberInfo, string> Properties = new()
    {
        {typeof(string).GetProperty(nameof(string.Length))!, "length"},
        {typeof(DateTime).GetProperty(nameof(DateTime.Year))!, "year"},
        {typeof(DateTime).GetProperty(nameof(DateTime.Month))!, "month"},
        {typeof(DateTime).GetProperty(nameof(DateTime.Day))!, "day_of_month"},
        {typeof(DateTime).GetProperty(nameof(DateTime.DayOfYear))!, "day_of_year"},
        {typeof(DateTime).GetProperty(nameof(DateTime.DayOfWeek))!, "-1 + day_of_week"},
        {typeof(DateTime).GetProperty(nameof(DateTime.Hour))!, "hour"},
        {typeof(DateTime).GetProperty(nameof(DateTime.Minute))!, "minute"},
        {typeof(DateTime).GetProperty(nameof(DateTime.Second))!, "second"}
    };

    /// <summary>
    /// Delegates dictionary.
    /// </summary>
    private static readonly Dictionary<MethodInfo, VisitMethodDelegate> Delegates = new List
            <KeyValuePair<MethodInfo?, VisitMethodDelegate>>
            {
                // TODO: Use nameof.
                GetStringMethod(nameof(string.ToLower), Type.EmptyTypes, GetFunc("lower")),
                GetStringMethod(nameof(string.ToUpper), Type.EmptyTypes, GetFunc("upper")),
                GetStringMethod(nameof(string.Contains), new[] {typeof(string)}, (e, v) => VisitSqlLike(e, v, "'%' || ? || '%'")),
                GetStringMethod(nameof(string.StartsWith), new[] {typeof(string)}, (e, v) => VisitSqlLike(e, v, "? || '%'")),
                GetStringMethod(nameof(string.EndsWith), new[] {typeof(string)}, (e, v) => VisitSqlLike(e, v, "'%' || ?")),
                GetStringMethod(nameof(string.IndexOf), new[] {typeof(string)}, GetFunc("instr", -1)),
                GetStringMethod(nameof(string.IndexOf), new[] {typeof(string), typeof(int)}, GetFunc("instr", -1)),
                GetStringMethod(nameof(string.Substring), new[] {typeof(int)}, GetFunc("substring", 0, 1)),
                GetStringMethod(nameof(string.Substring), new[] {typeof(int), typeof(int)}, GetFunc("substring", 0, 1)),
                GetStringMethod(nameof(string.Trim), "trim"),
                GetParameterizedTrimMethod(nameof(string.Trim), "trim"),
                GetParameterizedTrimMethod(nameof(string.TrimStart), "ltrim"),
                GetParameterizedTrimMethod(nameof(string.TrimEnd), "rtrim"),
                GetCharTrimMethod(nameof(string.Trim), "trim"),
                GetCharTrimMethod(nameof(string.TrimStart), "ltrim"),
                GetCharTrimMethod(nameof(string.TrimEnd), "rtrim"),
                GetStringMethod(nameof(string.Replace), "replace", typeof(string), typeof(string)),
                GetStringMethod(nameof(string.PadLeft), "lpad", typeof(int)),
                GetStringMethod(nameof(string.PadLeft), "lpad", typeof(int), typeof(char)),
                GetStringMethod(nameof(string.PadRight), "rpad", typeof(int)),
                GetStringMethod(nameof(string.PadRight), "rpad", typeof(int), typeof(char)),
                GetStringMethod(nameof(string.Compare), new[] { typeof(string), typeof(string) }, (e, v) => VisitStringCompare(e, v, false)),
                GetStringMethod(nameof(string.Compare), new[] { typeof(string), typeof(string), typeof(bool) }, (e, v) => VisitStringCompare(e, v, GetStringCompareIgnoreCaseParameter(e.Arguments[2]))),

                GetRegexMethod(nameof(Regex.Replace), "regexp_replace", typeof(string), typeof(string), typeof(string)),
                GetRegexMethod(nameof(Regex.Replace), "regexp_replace", typeof(string), typeof(string), typeof(string), typeof(RegexOptions)),
                GetRegexMethod(nameof(Regex.IsMatch), "regexp_like", typeof(string), typeof(string)),
                GetRegexMethod(nameof(Regex.IsMatch), "regexp_like", typeof(string), typeof(string), typeof(RegexOptions)),

                GetMethod(typeof(DateTime), "ToString", new[] {typeof(string)}, (e, v) => VisitFunc(e, v, "formatdatetime", ", 'en', 'UTC'")),

                GetMathMethod("Abs", typeof(int)),
                GetMathMethod("Abs", typeof(long)),
                GetMathMethod("Abs", typeof(float)),
                GetMathMethod("Abs", typeof(double)),
                GetMathMethod("Abs", typeof(decimal)),
                GetMathMethod("Abs", typeof(sbyte)),
                GetMathMethod("Abs", typeof(short)),
                GetMathMethod("Acos", typeof(double)),
                GetMathMethod("Acosh", typeof(double)),
                GetMathMethod("Asin", typeof(double)),
                GetMathMethod("Asinh", typeof(double)),
                GetMathMethod("Atan", typeof(double)),
                GetMathMethod("Atanh", typeof(double)),
                GetMathMethod("Atan2", typeof(double), typeof(double)),
                GetMathMethod("Ceiling", typeof(double)),
                GetMathMethod("Ceiling", typeof(decimal)),
                GetMathMethod("Cos", typeof(double)),
                GetMathMethod("Cosh", typeof(double)),
                GetMathMethod("Exp", typeof(double)),
                GetMathMethod("Floor", typeof(double)),
                GetMathMethod("Floor", typeof(decimal)),
                GetMathMethod("Log", typeof(double)),
                GetMathMethod("Log10", typeof(double)),
                GetMathMethod("Pow", "Power", typeof(double), typeof(double)),
                GetMathMethod("Round", typeof(double)),
                GetMathMethod("Round", typeof(double), typeof(int)),
                GetMathMethod("Round", typeof(decimal)),
                GetMathMethod("Round", typeof(decimal), typeof(int)),
                GetMathMethod("Sign", typeof(double)),
                GetMathMethod("Sign", typeof(decimal)),
                GetMathMethod("Sign", typeof(float)),
                GetMathMethod("Sign", typeof(int)),
                GetMathMethod("Sign", typeof(long)),
                GetMathMethod("Sign", typeof(short)),
                GetMathMethod("Sign", typeof(sbyte)),
                GetMathMethod("Sin", typeof(double)),
                GetMathMethod("Sinh", typeof(double)),
                GetMathMethod("Sqrt", typeof(double)),
                GetMathMethod("Tan", typeof(double)),
                GetMathMethod("Tanh", typeof(double)),
                GetMathMethod("Truncate", typeof(double)),
                GetMathMethod("Truncate", typeof(decimal)),
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

        visitor.AppendParameter(result);

        return true;
    }

    /// <summary>
    /// Gets the function.
    /// </summary>
    private static VisitMethodDelegate GetFunc(string func, params int[] adjust)
    {
        return (e, v) => VisitFunc(e, v, func, null, adjust);
    }

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

        for (int i= 0; i < expression.Arguments.Count; i++)
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
        string func)
    {
        visitor.ResultBuilder.Append(func).Append('(');

        visitor.Visit(expression.Object!);

        var arg = expression.Arguments[0];

        if (arg != null!)
        {
            visitor.ResultBuilder.Append(", ");

            if (arg.NodeType == ExpressionType.Constant)
            {
                var constant = (ConstantExpression) arg;

                if (constant.Value is char)
                {
                    visitor.AppendParameter((char) constant.Value);
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
                        throw new NotSupportedException("String.Trim function only supports a single argument: " +
                                                        expression);
                    }

                    visitor.AppendParameter(enumeratedArgs[0]);
                }
            }
            else
            {
                visitor.Visit(arg);
            }
        }

        visitor.ResultBuilder.Append(')');
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
        // Ex: nvl2(?, casewhen(_T0.NAME = ?, 0, casewhen(_T0.NAME >= ?, 1, -1)), 1)
        visitor.ResultBuilder.Append("nvl2(");
        visitor.Visit(expression.Arguments[1]);
        visitor.ResultBuilder.Append(", casewhen(");
        VisitArg(visitor, expression, 0, ignoreCase);
        visitor.ResultBuilder.Append(" = ");
        VisitArg(visitor, expression, 1, ignoreCase);
        visitor.ResultBuilder.Append(", 0, casewhen(");
        VisitArg(visitor, expression, 0, ignoreCase);
        visitor.ResultBuilder.Append(" >= ");
        VisitArg(visitor, expression, 1, ignoreCase);
        visitor.ResultBuilder.Append(", 1, -1)), 1)");
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
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetParameterizedTrimMethod(
        string name,
        string sqlName)
    {
        return GetMethod(
            typeof(string),
            name,
            new[] {typeof(char[])},
            (e, v) => VisitParameterizedTrimFunc(e, v, sqlName));
    }

    /// <summary>
    /// Gets string parameterized Trim(TrimStart, TrimEnd) method that takes a single char.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetCharTrimMethod(
        string name,
        string sqlName)
    {
        return GetMethod(
            typeof(string),
            name,
            new[] {typeof(char)},
            (e, v) => VisitParameterizedTrimFunc(e, v, sqlName));
    }

    /// <summary>
    /// Gets the math method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetMathMethod(
        string name,
        string sqlName,
        params Type[] argTypes)
    {
        return GetMethod(typeof(Math), name, argTypes, GetFunc(sqlName));
    }

    /// <summary>
    /// Gets the math method.
    /// </summary>
    private static KeyValuePair<MethodInfo?, VisitMethodDelegate> GetMathMethod(string name, params Type[] argTypes)
    {
        return GetMathMethod(name, name, argTypes);
    }
}
