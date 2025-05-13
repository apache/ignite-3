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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests for data streamer with .NET receiver.
/// </summary>
public class DataStreamerPlatformReceiverTests : IgniteTestsBase
{
    private DeploymentUnit _defaultTestUnit = null!;

    [OneTimeSetUp]
    public async Task DeployDefaultUnit() => _defaultTestUnit = await ManagementApi.DeployTestsAssembly();

    [OneTimeTearDown]
    public async Task UndeployDefaultUnit() => await ManagementApi.UnitUndeploy(_defaultTestUnit);

    [TestCaseSource(typeof(TestCases), nameof(TestCases.SupportedArgs))]
    public async Task TestEchoReceiverAllDataTypes(object item)
    {
        List<object> items = [item, item];
        var res = await RunEchoReceiver(items);

        var expected = items.Select(x => x is decimal dec ? new BigDecimal(dec) : x).ToList();

        CollectionAssert.AreEqual(expected, res);
    }

    [TestCaseSource(typeof(TestCases), nameof(TestCases.SupportedArgs))]
    public async Task TestEchoArgsReceiverAllDataTypes(object arg)
    {
        var res = await RunEchoArgReceiver(arg);

        if (arg is decimal dec)
        {
            arg = new BigDecimal(dec);
        }

        Assert.AreEqual(arg, res);
    }

    [Test]
    public async Task TestEchoArgsReceiverTupleWithSchema()
    {
        var arg = TestCases.GetTupleWithAllFieldTypes(x => x is not decimal);
        var res = await RunEchoArgReceiver(arg);

        Assert.AreEqual(arg, res);
    }

    [Test]
    public async Task TestEchoReceiverTupleWithSchema()
    {
        var arg = TestCases.GetTupleWithAllFieldTypes(x => x is not decimal);
        List<object> items = [arg];

        var res = await RunEchoReceiver(items);

        Assert.AreEqual(items, res);
    }

    [Test]
    public void TestMissingClass()
    {
        var receiverDesc = new ReceiverDescriptor<object, object>("BadClass")
        {
            Options = new ReceiverExecutionOptions
            {
                ExecutorType = JobExecutorType.DotNetSidecar
            }
        };

        IAsyncEnumerable<object> resStream = PocoView.StreamDataAsync<object, object, object, object>(
            new object[] { 1 }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x.ToString()!,
            receiverDesc,
            receiverArg: "arg");

        var ex = Assert.ThrowsAsync<DataStreamerException>(async () => await resStream.SingleAsync());
        Assert.AreEqual(".NET job failed: Type 'BadClass' not found in the specified deployment units.", ex.Message);
        Assert.AreEqual(1, ex.FailedItems.Count);
    }

    [Test]
    public void TestMissingAssembly()
    {
        var receiverDesc = new ReceiverDescriptor<object>("MyClass, BadAssembly")
        {
            Options = new ReceiverExecutionOptions
            {
                ExecutorType = JobExecutorType.DotNetSidecar
            }
        };

        var task = PocoView.StreamDataAsync<object, object, object>(
            new object[] { 1 }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x.ToString()!,
            receiverDesc,
            receiverArg: "arg");

        var ex = Assert.ThrowsAsync<DataStreamerException>(async () => await task);

        Assert.AreEqual(
            ".NET job failed: Could not load file or assembly 'BadAssembly, Culture=neutral, PublicKeyToken=null'. " +
            "The system cannot find the file specified.",
            ex.Message.Trim());

        Assert.AreEqual(1, ex.FailedItems.Count);
    }

    [Test]
    public void TestReceiverError()
    {
        IAsyncEnumerable<object> resStream = PocoView.StreamDataAsync<object, object, object, object>(
            new object[] { 1 }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x.ToString()!,
            DotNetReceivers.Error with { DeploymentUnits = [_defaultTestUnit] },
            receiverArg: "hello");

        var ex = Assert.ThrowsAsync<DataStreamerException>(async () => await resStream.SingleAsync());
        Assert.AreEqual(".NET job failed: Error in receiver: hello", ex.Message);
        Assert.AreEqual("IGN-CATALOG-1", ex.CodeAsString);
        Assert.AreEqual(1, ex.FailedItems.Count);
    }

    [Test]
    public async Task TestRunDotNetReceiverFromJava()
    {
        await Task.Delay(1);
        Assert.Fail("TODO");
    }

    [Test]
    public async Task TestIgniteApiAccessFromReceiver()
    {
        var ids = Enumerable.Range(10, 50).ToList();
        var tableName = nameof(TestIgniteApiAccessFromReceiver);

        var res = await TupleView.StreamDataAsync(
            data: ids.ToAsyncEnumerable(),
            keySelector: _ => new IgniteTuple(),
            payloadSelector: id => id,
            receiver: DotNetReceivers.CreateTableAndInsert,
            receiverArg: tableName,
            options: new DataStreamerOptions { PageSize = 33 }).ToListAsync();

        Assert.AreEqual(ids.Count, res.Count);
    }

    [Test]
    public async Task TestEchoManyItems([Values(1, 3, 99, 100_000)] int pageSize)
    {
        const int count = 3_000;

        var items = Enumerable.Range(0, count)
            .Select(x => new IgniteTuple { ["id"] = x, ["name"] = $"foo-{x}" })
            .ToList();

        var res = await RunEchoReceiver(items);

        CollectionAssert.AreEqual(items, res);
    }

    private async Task<object> RunEchoArgReceiver(object arg) =>
        await PocoView.StreamDataAsync<object, object, object, object>(
            new object[] { "unused" }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: _ => "unused",
            DotNetReceivers.EchoArgs with { DeploymentUnits = [_defaultTestUnit] },
            receiverArg: arg).SingleAsync();

    private async Task<List<object>> RunEchoReceiver(IEnumerable<object> items) =>
        await PocoView.StreamDataAsync<object, object, object, object>(
            items.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x,
            DotNetReceivers.Echo with { DeploymentUnits = [_defaultTestUnit] },
            receiverArg: "unused").ToListAsync();
}
