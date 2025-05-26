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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Marshalling;
using Ignite.Table;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests for data streamer with .NET receiver.
/// </summary>
public class DataStreamerPlatformReceiverTests : IgniteTestsBase
{
    private static readonly JobDescriptor<JobInfo, object?> StreamerRunnerJob = new(
        ComputeTests.PlatformTestNodeRunner + "$StreamerRunnerJob")
    {
        ArgMarshaller = new JsonMarshaller<JobInfo>()
    };

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
        StringAssert.StartsWith(".NET job failed: Failed to load type 'BadClass'", ex.Message);
        StringAssert.Contains("Could not resolve type 'BadClass' in assembly 'Apache.Ignite", ex.Message);
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

        StringAssert.Contains(".NET job failed: Failed to load type 'MyClass, BadAssembly'", ex.Message);
        StringAssert.Contains("Could not load file or assembly 'BadAssembly", ex.Message);
        StringAssert.Contains("The system cannot find the file specified.", ex.Message);

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
        var jobTarget = JobTarget.AnyNode(await Client.GetClusterNodesAsync());

        var jobInfo = new JobInfo(
            TypeName: typeof(DotNetReceivers.UpdateTupleReceiver).AssemblyQualifiedName!,
            Arg: "hello",
            DeploymentUnits: [$"{_defaultTestUnit.Name}:{_defaultTestUnit.Version}"],
            JobExecutorType: "DOTNET_SIDECAR");

        var jobExec = await Client.Compute.SubmitAsync(jobTarget, StreamerRunnerJob, jobInfo);
        var res = await jobExec.GetResultAsync();

        Assert.AreEqual("Streaming finished: TupleImpl [VAL=java-test, VAL2=dotnet-test]", res);
    }

    [Test]
    public async Task TestIgniteApiAccessFromReceiver()
    {
        var ids = Enumerable.Range(10, 50).ToList();
        var tableName = nameof(TestIgniteApiAccessFromReceiver);

        await Client.Sql.ExecuteAsync(null, $"DROP TABLE IF EXISTS {tableName}");

        var res = await TupleView.StreamDataAsync(
            data: ids.ToAsyncEnumerable(),
            keySelector: _ => new IgniteTuple { ["key"] = 1L },
            payloadSelector: id => id,
            receiver: DotNetReceivers.CreateTableAndUpsert with {DeploymentUnits = [_defaultTestUnit] },
            receiverArg: tableName,
            options: new DataStreamerOptions { PageSize = 13 }).ToListAsync();

        Assert.AreEqual(ids.Count, res.Count);

        // TODO IGNITE-24659 Client does not observe table changes from streamer receiver: remove WaitForConditionAsync.
        await TestUtils.WaitForConditionAsync(async () =>
        {
            await using var resultSet = await Client.Sql.ExecuteAsync(null, $"SELECT * FROM {tableName}");
            var rows = await resultSet.ToListAsync();

            return rows.Count == ids.Count;
        });
    }

    [Test]
    public async Task TestEchoManyItems([Values(1, 3, 99, 100_000)] int pageSize)
    {
        const int count = 1_000;

        var items = Enumerable.Range(0, count)
            .Select(x => new IgniteTuple { ["id"] = x, ["name"] = $"foo-{x}" })
            .ToList();

        var res = await RunEchoReceiver(items, new DataStreamerOptions { PageSize = pageSize });

        CollectionAssert.AreEqual(items, res);
    }

    [Test]
    public async Task TestPlatformExecutorWithOldServerThrowsCompatibilityError()
    {
        using var server = new FakeServer();
        using var client = await server.ConnectClientAsync();

        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var view = table!.RecordBinaryView;

        var ex = Assert.ThrowsAsync<DataStreamerException>(async () => await view.StreamDataAsync<object, object, object, object>(
            new object[] { "unused" }.ToAsyncEnumerable(),
            keySelector: _ => new IgniteTuple { ["ID"] = 1 },
            payloadSelector: _ => "unused",
            DotNetReceivers.EchoArgs with { DeploymentUnits = [_defaultTestUnit] },
            receiverArg: "test").SingleAsync());

        Assert.AreEqual("ReceiverExecutionOptions are not supported by the server.", ex.Message);
        Assert.AreEqual(1, ex.FailedItems.Count);
    }

    private async Task<object> RunEchoArgReceiver(object arg, IRecordView<Poco>? view = null)
    {
        view ??= PocoView;

        return await view.StreamDataAsync<object, object, object, object>(
            new object[] { "unused" }.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: _ => "unused",
            DotNetReceivers.EchoArgs with { DeploymentUnits = [_defaultTestUnit] },
            receiverArg: arg).SingleAsync();
    }

    private async Task<List<object>> RunEchoReceiver(IEnumerable<object> items, DataStreamerOptions? options = null) =>
        await PocoView.StreamDataAsync<object, object, object, object>(
            items.ToAsyncEnumerable(),
            keySelector: _ => new Poco(),
            payloadSelector: x => x,
            DotNetReceivers.Echo with { DeploymentUnits = [_defaultTestUnit] },
            receiverArg: "unused",
            options).ToListAsync();

    [SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Local", Justification = "JSON")]
    private record JobInfo(string TypeName, object Arg, List<string> DeploymentUnits, string JobExecutorType);
}
