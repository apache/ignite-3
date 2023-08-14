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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

/// <summary>
/// Tests client-side schema validation.
/// Client is responsible for unmapped column checks.
/// </summary>
public class SchemaValidationTest : IgniteTestsBase
{
    [Test]
    public void TestUnmappedTupleFields()
    {
        var igniteTuple = new IgniteTuple
        {
            [KeyCol] = 1L,
            [ValCol] = "v",
            ["foo"] = "abc",
            ["bar"] = null
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await TupleView.UpsertAsync(null, igniteTuple));
        StringAssert.StartsWith("Tuple doesn't match schema: schemaVersion=1, extraColumns=FOO, BAR ", ex!.Message);
    }

    [Test]
    public void TestKvUnmappedKeyTupleFields()
    {
        var keyTuple = new IgniteTuple
        {
            [KeyCol] = 1L,
            ["bar"] = null
        };

        var valTuple = new IgniteTuple
        {
            [ValCol] = "v"
        };

        var kvView = Table.KeyValueBinaryView;
        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await kvView.PutAsync(null, keyTuple, valTuple));
        StringAssert.StartsWith("Tuple pair doesn't match schema: schemaVersion=1, extraColumns=BAR ", ex!.Message);
    }

    [Test]
    public void TestKvUnmappedValTupleFields()
    {
        var keyTuple = new IgniteTuple
        {
            [KeyCol] = 1L
        };

        var valTuple = new IgniteTuple
        {
            [ValCol] = "v",
            ["baz"] = 0
        };

        var kvView = Table.KeyValueBinaryView;
        var ex = Assert.ThrowsAsync<ArgumentException>(async () => await kvView.PutAsync(null, keyTuple, valTuple));
        StringAssert.StartsWith("Tuple pair doesn't match schema: schemaVersion=1, extraColumns=BAZ ", ex!.Message);
    }

    [Test]
    public void TestMissingKeyTupleFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestMissingValTupleFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvMissingKeyTupleFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvMissingValTupleFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestUnmappedPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvUnmappedKeyPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvUnmappedValPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestMissingKeyPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestMissingValPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvMissingKeyPocoFields()
    {
        Assert.Fail("TODO");
    }

    [Test]
    public void TestKvMissingValPocoFields()
    {
        Assert.Fail("TODO");
    }
}
