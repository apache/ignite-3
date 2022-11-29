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
    public void TestModulus()
    {
        // TODO: Calcite throws errors - asked in #sql_team.
        TestOpDouble(x => x.Key % 3d, 1.0d, "select (_T0.KEY % ?) from");
        TestOpInt(x => x.Key % 4, 2, "select (_T0.KEY % ?) from");
    }

    [Test]
    public void TestNumericFunctions()
    {
        Assert.Fail("TODO");
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

    private static void TestOp<T>(IRecordView<T> view, Expression<Func<T, double>> expr, double expectedRes, string expectedQuery)
        where T : notnull
    {
        var query = view.AsQueryable().Select(expr);
        var res = query.Max();

        Assert.AreEqual(expectedRes, res);
        StringAssert.Contains(expectedQuery, query.ToString());
    }

    private void TestOpDouble(Expression<Func<PocoDouble, double>> expr, double expectedRes, string expectedQuery) =>
        TestOp(PocoDoubleView, expr, expectedRes, expectedQuery);

    private void TestOpInt(Expression<Func<PocoInt, double>> expr, double expectedRes, string expectedQuery) =>
        TestOp(PocoIntView, expr, expectedRes, expectedQuery);
}
