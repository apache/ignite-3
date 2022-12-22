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
using System.Threading.Tasks;
using Ignite.Table;
using NodaTime;
using NUnit.Framework;
using Table;

/// <summary>
/// Linq functions tests.
/// </summary>
[SuppressMessage("Globalization", "CA1304:Specify CultureInfo", Justification = "SQL")]
[SuppressMessage("Globalization", "CA1309:Use ordinal string comparison", Justification = "SQL")]
[SuppressMessage("ReSharper", "StringIndexOfIsCultureSpecific.1", Justification = "SQL")]
[SuppressMessage("ReSharper", "StringIndexOfIsCultureSpecific.2", Justification = "SQL")]
[SuppressMessage("ReSharper", "StringCompareToIsCultureSpecific", Justification = "SQL")]
[SuppressMessage("ReSharper", "StringCompareIsCultureSpecific.1", Justification = "SQL")]
[SuppressMessage("ReSharper", "StringCompareIsCultureSpecific.2", Justification = "SQL")]
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
        TestOpString(x => x.Val + "_", "v-9_", "select concat(_T0.VAL, ?) from");
        TestOpString(x => "_" + x.Val, "_v-9", "select concat(?, _T0.VAL) from");
        TestOpString(x => "[" + x.Val + "]", "[v-9]", "select concat(concat(?, _T0.VAL), ?) from");

        TestOpString(x => x.Val!.ToUpper(), "V-9", "select upper(_T0.VAL) from");
        TestOpString(x => x.Val!.ToLower(), "v-9", "select lower(_T0.VAL) from");

        TestOpString(x => x.Val!.Substring(1), "-9", "select substring(_T0.VAL, ? + 1) from");
        TestOpString(x => x.Val!.Substring(0, 2), "v-", "select substring(_T0.VAL, 0 + 1, 2) from");

        TestOpString(x => x.Val!.Trim(), "v-9", "select trim(_T0.VAL) from");
        TestOpString(x => x.Val!.TrimStart(), "v-9", "select ltrim(_T0.VAL) from");
        TestOpString(x => x.Val!.TrimEnd(), "v-9", "select rtrim(_T0.VAL) from");

        TestOpString(x => x.Val!.Trim('v'), "-9", "select trim(both ? from _T0.VAL) from");
        TestOpString(x => x.Val!.TrimStart('v'), "-9", "select trim(leading ? from _T0.VAL) from");
        TestOpString(x => x.Val!.TrimEnd('9'), "v-8", "select trim(trailing ? from _T0.VAL) from");

        TestOpString(x => x.Val!.Contains("v-"), true, "select (_T0.VAL like '%' || ? || '%') from");
        TestOpString(x => x.Val!.StartsWith("v-"), true, "select (_T0.VAL like ? || '%') from");
        TestOpString(x => x.Val!.EndsWith("-9"), true, "select (_T0.VAL like '%' || ?) from");

        TestOpString(x => x.Val!.IndexOf("-9"), 1, "select -1 + position(? in _T0.VAL) from");
        TestOpString(x => x.Val!.IndexOf("-9", 2), -1, "select -1 + position(? in _T0.VAL from (? + 1)) from");

        TestOpString(x => x.Val!.Length, 3, "select length(_T0.VAL) from");

        TestOpString(x => x.Val!.Replace("v-", "x + "), "x + 9", "select replace(_T0.VAL, ?, ?) from");

        TestOpString(
            x => string.Compare(x.Val, "abc"),
            1,
            "select case when (_T0.VAL is not distinct from ?) then 0 else (case when (_T0.VAL > ?) then 1 else -1 end) end from");

        TestOpString(
            x => string.Compare(x.Val, "abc", true),
            1,
            "select case when (_T0.VAL is not distinct from ?) then 0 else (case when (_T0.VAL > ?) then 1 else -1 end) end from");
    }

    [Test]
    public async Task TestDateFunctions()
    {
        var dates = (await Client.Tables.GetTableAsync(TableDateName))!.GetRecordView<PocoDate>();
        var localDate = new LocalDate(2022, 12, 20);
        await dates.UpsertAsync(null, new PocoDate(localDate, localDate));

        // TODO: year, month, day_of_month, day_of_week, day_of_year, hour, minute, second
        // TODO: LocalTime, LocalDateTime
        // TODO: Test projecting all parts into anonymous type.
        TestOp(dates, x => x.Key.Day, 20, "select dayofmonth(_T0.KEY) from");
        TestOp(dates, x => x.Key.DayOfYear, 354, "select dayofyear(_T0.KEY) from");
        TestOp(dates, x => x.Key.Day, 20, "select dayofmonth(_T0.KEY) from");
        TestOp(dates, x => x.Key.DayOfWeek, IsoDayOfWeek.Tuesday, "select -1 + dayofweek(_T0.KEY) from");
        TestOp(dates, x => (int)x.Key.DayOfWeek, (int)IsoDayOfWeek.Tuesday, "select cast(-1 + dayofweek(_T0.KEY) as int) from");
    }

    [Test]
    public async Task TestDateTimeFunctions()
    {
        var dateTimes = (await Client.Tables.GetTableAsync(TableDateTimeName))!.GetRecordView<PocoDateTime>();
        var localDateTime = new LocalDateTime(2022, 12, 20, 20, 07, 36, 123);
        await dateTimes.UpsertAsync(null, new PocoDateTime(localDateTime, localDateTime));

        TestOp(dateTimes, x => x.Key.Day, 20, "select dayofmonth(_T0.KEY) from");
        TestOp(dateTimes, x => x.Key.DayOfYear, 354, "select dayofyear(_T0.KEY) from");
        TestOp(dateTimes, x => x.Key.Day, 20, "select dayofmonth(_T0.KEY) from");
        TestOp(dateTimes, x => x.Key.DayOfWeek, IsoDayOfWeek.Tuesday, "select -1 + dayofweek(_T0.KEY) from");
        TestOp(dateTimes, x => (int)x.Key.DayOfWeek, (int)IsoDayOfWeek.Tuesday, "select cast(-1 + dayofweek(_T0.KEY) as int) from");
        TestOp(dateTimes, x => x.Key.Hour, 20, "select hour(_T0.KEY) from");
        TestOp(dateTimes, x => x.Key.Minute, 7, "select minute(_T0.KEY) from");
        TestOp(dateTimes, x => x.Key.Second, 36, "select second(_T0.KEY) from");
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
