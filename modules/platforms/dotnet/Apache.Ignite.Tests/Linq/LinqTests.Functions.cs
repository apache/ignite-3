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

namespace Apache.Ignite.Tests.Linq;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using Ignite.Table;
using NUnit.Framework;
using Table;

/// <summary>
/// Linq type cast tests.
/// </summary>
[SuppressMessage("Globalization", "CA1304:Specify CultureInfo", Justification = "SQL")]
[SuppressMessage("ReSharper", "StringIndexOfIsCultureSpecific.1", Justification = "SQL")]
[SuppressMessage("ReSharper", "StringIndexOfIsCultureSpecific.2", Justification = "SQL")]
public partial class LinqTests
{
    [Test]
    public void TestNumericOperators()
    {
        TestOpDouble(x => x.Key + 1.234d, 10.234d, "select (_T0.KEY + ?) from");
        TestOpDouble(x => x.Key - 1.234d, 7.766d, "select (_T0.KEY - ?) from");
        TestOpDouble(x => x.Key * 2.5d, 22.5d, "select (_T0.KEY * ?) from");
        TestOpDouble(x => x.Key / 3d, 3.0d, "select (_T0.KEY / ?) from");

        TestOpInt(x => x.Key + 1.234d, 10.234d, "select (cast(_T0.KEY as double) + ?) from");
        TestOpInt(x => x.Key - 1.234d, 7.766d, "select (cast(_T0.KEY as double) - ?) from");
        TestOpInt(x => x.Key * 2.5d, 22.5d, "select (cast(_T0.KEY as double) * ?) from");
        TestOpInt(x => x.Key / 3d, 3.0d, "select (cast(_T0.KEY as double) / ?) from");
    }

    [Test]
    [Ignore("IGNITE-18274")]
    public void TestRemainder()
    {
        TestOpDouble(x => x.Key % 3d, 1.0d, "select (_T0.KEY % ?) from");
        TestOpInt(x => x.Key % 4, 2, "select (_T0.KEY % ?) from");
    }

    [Test]
    public void TestNumericFunctions()
    {
        // ACOSH, ASINH, ATANH, ATAN2, LOG2, LOG(X, Y) are not supported by Calcite.
        TestOpDouble(x => Math.Abs(-x.Key), 9.0d, "select Abs((-_T0.KEY)) from");
        TestOpDouble(x => Math.Cos(x.Key + 2), 0.96017028665036597d, "select Cos((_T0.KEY + ?)) from");
        TestOpDouble(x => Math.Cosh(x.Key), 4051.5420254925943d, "select Cosh(_T0.KEY) from");
        TestOpDouble(x => Math.Acos(x.Key / 100), 1.5707963267948966d, "select Acos((_T0.KEY / ?)) from");
        TestOpDouble(x => Math.Sin(x.Key), 0.98935824662338179d, "select Sin(_T0.KEY) from");
        TestOpDouble(x => Math.Sinh(x.Key), 4051.5419020827899d, "select Sinh(_T0.KEY) from");
        TestOpDouble(x => Math.Asin(x.Key / 100), 0.090121945014595251d, "select Asin((_T0.KEY / ?)) from");
        TestOpDouble(x => Math.Tan(x.Key), 1.5574077246549023d, "select Tan(_T0.KEY) from");
        TestOpDouble(x => Math.Tanh(x.Key / 10), 0.71629787019902447d, "select Tanh((_T0.KEY / ?)) from");
        TestOpDouble(x => Math.Atan(x.Key), 1.4601391056210009d, "select Atan(_T0.KEY) from");
        TestOpDouble(x => Math.Ceiling(x.Key / 3), 3.0d, "select Ceiling((_T0.KEY / ?)) from");
        TestOpDouble(x => Math.Exp(x.Key), 8103.0839275753842d, "select Exp(_T0.KEY) from");
        TestOpDouble(x => Math.Log(x.Key), 2.1972245773362196d, "select Ln(_T0.KEY) from");
        TestOpDouble(x => Math.Log10(x.Key), 0.95424250943932487d, "select Log10(_T0.KEY) from");
        TestOpDouble(x => Math.Pow(x.Key, 2), 81, "select Power(_T0.KEY, 2) from");
        TestOpDouble(x => Math.Round(x.Key / 5), 2, "select Round((_T0.KEY / ?)) from");
        TestOpDouble(x => Math.Sign(x.Key - 10), -1, "select Sign((_T0.KEY - ?)) from");
        TestOpDouble(x => Math.Sqrt(x.Key), 3.0d, "select Sqrt(_T0.KEY) from");
        TestOpDouble(x => Math.Truncate(x.Key + 0.8), 9.0d, "select Truncate((_T0.KEY + ?)) from");
    }

    [Test]
    public void TestStringFunctions()
    {
        TestOpString(x => x.Val!.ToUpper(), "V-9", "select upper(_T0.VAL) from");
        TestOpString(x => x.Val!.ToLower(), "v-9", "select lower(_T0.VAL) from");

        TestOpString(x => x.Val!.Substring(1), "-9", "select substring(_T0.VAL, ? + 1) from");
        TestOpString(x => x.Val!.Substring(0, 2), "v-", "select substring(_T0.VAL, 0 + 1, 2) from");
        TestOpString(x => x.Val!.Trim(), "v-9", "select trim(_T0.VAL) from");
        TestOpString(x => x.Val!.TrimStart(), "v-9", "select ltrim(_T0.VAL) from");
        TestOpString(x => x.Val!.TrimEnd(), "v-9", "select rtrim(_T0.VAL) from");

        Assert.Fail("TODO");
    }

    [Test]
    [Ignore("IGNITE-18283 Exception on SQL LIKE")]
    public void TestStringFunctionsIgnored()
    {
        // We can't use inlining workaround with LIKE (as we do with Math.Log) - with strings it will allow SQL injection.
        TestOpString(x => x.Val!.Contains("v-"), true, "select (_T0.VAL like '%v-%') from");
        TestOpString(x => x.Val!.StartsWith("v-"), true, "select (_T0.VAL like ? || '%') from");
        TestOpString(x => x.Val!.EndsWith("-9"), true, "select (_T0.VAL like '%' || ?) from");

        TestOpString(x => x.Val + "_", "v-9_", "select concat(_T0.VAL, ?) from");
        TestOpString(x => x.Val!.IndexOf("-9"), 1, "select instr(_T0.VAL, ?) -1 from");
        TestOpString(x => x.Val!.IndexOf("-9", 1), -1, "select instr(_T0.VAL, ?, ?) -1 from");
    }

    [Test]
    public void TestDateFunctions()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestRegex()
    {
        Assert.Fail("TODO");
    }

    private static void TestOp<T, TRes>(IRecordView<T> view, Expression<Func<T, TRes>> expr, TRes expectedRes, string expectedQuery)
        where T : notnull
    {
        var query = view.AsQueryable().Select(expr);
        var sql = query.ToString();

        TRes? res;

        try
        {
            res = query.Max();
        }
        catch (Exception e)
        {
            throw new Exception("Failed to execute query: " + sql, e);
        }

        Assert.Multiple(() =>
        {
            Assert.AreEqual(expectedRes, res);
            StringAssert.Contains(expectedQuery, sql);
        });
    }

    private void TestOpDouble<TRes>(Expression<Func<PocoDouble, TRes>> expr, TRes expectedRes, string expectedQuery) =>
        TestOp(PocoDoubleView, expr, expectedRes, expectedQuery);

    private void TestOpInt<TRes>(Expression<Func<PocoInt, TRes>> expr, TRes expectedRes, string expectedQuery) =>
        TestOp(PocoIntView, expr, expectedRes, expectedQuery);

    private void TestOpString<TRes>(Expression<Func<Poco, TRes>> expr, TRes expectedRes, string expectedQuery) =>
        TestOp(PocoView, expr, expectedRes, expectedQuery);
}
