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
using System.Linq;
using System.Linq.Expressions;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Linq type cast tests.
/// </summary>
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
    public void TestModulus()
    {
        TestOpDouble(x => x.Key % 3d, 1.0d, "select (_T0.KEY % ?) from");
        TestOpInt(x => x.Key % 4, 2, "select (_T0.KEY % ?) from");
    }

    [Test]
    public void TestNumericFunctions()
    {
        // TODO: ACOSH, ASINH are not supported, but COSH and SINH are?
        TestOpDouble(x => Math.Abs(-x.Key), 9.0d, "select Abs((-_T0.KEY)) from");
        TestOpDouble(x => Math.Cos(x.Key + 2), 0.96017028665036597d, "select Cos((_T0.KEY + ?)) from");
        TestOpDouble(x => Math.Cosh(x.Key), 4051.5420254925943d, "select Cosh(_T0.KEY) from");
        TestOpDouble(x => Math.Acos(x.Key / 100), 1.5707963267948966d, "select Acos((_T0.KEY / ?)) from");
        TestOpDouble(x => Math.Sin(x.Key), 0.98935824662338179d, "select Sin(_T0.KEY) from");
        TestOpDouble(x => Math.Sinh(x.Key), 4051.5419020827899d, "select Sinh(_T0.KEY) from");
        TestOpDouble(x => Math.Asin(x.Key / 100), 0.090121945014595251d, "select Asin((_T0.KEY / ?)) from");
        TestOpDouble(x => Math.Tan(x.Key), 9.0d, "select Tan(_T0.KEY) from");
        TestOpDouble(x => Math.Tanh(x.Key), 9.0d, "select Tanh(_T0.KEY) from");
        TestOpDouble(x => Math.Atan(x.Key), 9.0d, "select Atan(_T0.KEY) from");
        TestOpDouble(x => Math.Atanh(x.Key), 9.0d, "select Atanh(_T0.KEY) from");
        TestOpDouble(x => Math.Atan2(x.Key, 0.5), 9.0d, "select Atan2(_T0.KEY) from");

        // TODO: Ceiling, Exp, Floor, Exp, Log, Log10, Pow, Round, Sign, Sqrt, Truncate
        TestOpInt(x => Math.Abs(-x.Key), 9, "select Abs((-_T0.KEY)) from");
    }

    [Test]
    public void TestStringFunctions()
    {
        Assert.Fail("TODO");
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
        var res = query.Max();

        Assert.Multiple(() =>
        {
            Assert.AreEqual(expectedRes, res);
            StringAssert.Contains(expectedQuery, query.ToString());
        });
    }

    private void TestOpDouble<TRes>(Expression<Func<PocoDouble, TRes>> expr, TRes expectedRes, string expectedQuery) =>
        TestOp(PocoDoubleView, expr, expectedRes, expectedQuery);

    private void TestOpInt<TRes>(Expression<Func<PocoInt, TRes>> expr, TRes expectedRes, string expectedQuery) =>
        TestOp(PocoIntView, expr, expectedRes, expectedQuery);
}
