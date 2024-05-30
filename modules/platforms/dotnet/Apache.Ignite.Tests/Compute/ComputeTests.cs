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

namespace Apache.Ignite.Tests.Compute
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Net;
    using System.Numerics;
    using System.Threading.Tasks;
    using Ignite.Compute;
    using Ignite.Table;
    using Internal.Compute;
    using Internal.Network;
    using Internal.Proto;
    using Network;
    using NodaTime;
    using NUnit.Framework;
    using Table;

    /// <summary>
    /// Tests <see cref="ICompute"/>.
    /// </summary>
    public class ComputeTests : IgniteTestsBase
    {
        public const string NodeNameJob = ItThinClientComputeTest + "$NodeNameJob";

        public const string PlatformTestNodeRunner = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner";

        private const string ItThinClientComputeTest = "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest";

        private const string ConcatJob = ItThinClientComputeTest + "$ConcatJob";

        private const string ErrorJob = ItThinClientComputeTest + "$IgniteExceptionJob";

        private const string EchoJob = ItThinClientComputeTest + "$EchoJob";

        private const string SleepJob = ItThinClientComputeTest + "$SleepJob";

        private const string DecimalJob = ItThinClientComputeTest + "$DecimalJob";

        private const string CreateTableJob = PlatformTestNodeRunner + "$CreateTableJob";

        private const string DropTableJob = PlatformTestNodeRunner + "$DropTableJob";

        private const string ExceptionJob = PlatformTestNodeRunner + "$ExceptionJob";

        private const string CheckedExceptionJob = PlatformTestNodeRunner + "$CheckedExceptionJob";

        private static readonly IList<DeploymentUnit> Units = Array.Empty<DeploymentUnit>();

        [Test]
        public async Task TestGetClusterNodes()
        {
            var res = (await Client.GetClusterNodesAsync()).OrderBy(x => x.Name).ToList();

            Assert.AreEqual(4, res.Count);

            Assert.IsNotEmpty(res[0].Id);
            Assert.IsNotEmpty(res[1].Id);

            Assert.AreEqual(3344, res[0].Address.Port);
            Assert.AreEqual(3345, res[1].Address.Port);

            Assert.AreNotEqual(IPAddress.None, res[0].Address.Address);
            Assert.AreNotEqual(IPAddress.None, res[1].Address.Address);
            Assert.AreEqual(res[0].Address.Address, res[1].Address.Address);
        }

        [Test]
        public async Task TestExecuteOnSpecificNode()
        {
            var res1 = await Client.Compute.SubmitAsync<string>(await GetNodeAsync(0), Units, NodeNameJob, "-", 11);
            var res2 = await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), Units, NodeNameJob, ":", 22);

            Assert.AreEqual(PlatformTestNodeRunner + "-_11", await res1.GetResultAsync());
            Assert.AreEqual(PlatformTestNodeRunner + "_2:_22", await res2.GetResultAsync());
        }

        [Test]
        public async Task TestExecuteOnRandomNode()
        {
            var jobExecution = await Client.Compute.SubmitAsync<string>(await Client.GetClusterNodesAsync(), Units, NodeNameJob);
            var res = await jobExecution.GetResultAsync();

            var expectedNodeNames = Enumerable.Range(1, 4)
                .Select(x => x == 1 ? PlatformTestNodeRunner : PlatformTestNodeRunner + "_" + x)
                .ToList();

            CollectionAssert.Contains(expectedNodeNames, res);
        }

        [Test]
        public async Task TestExecuteResultTypeMismatchThrowsInvalidCastException()
        {
            var jobExecution = await Client.Compute.SubmitAsync<Guid>(await Client.GetClusterNodesAsync(), Units, NodeNameJob);
            Assert.ThrowsAsync<InvalidCastException>(async () => await jobExecution.GetResultAsync());
        }

        [Test]
        public async Task TestBroadcastOneNode()
        {
            var nodes = await GetNodeAsync(0);

            IDictionary<IClusterNode, Task<IJobExecution<string>>> taskMap = Client.Compute.SubmitBroadcast<string>(
                nodes,
                Units,
                NodeNameJob,
                JobExecutionOptions.Default,
                "123");
            var res = await taskMap[nodes[0]];

            Assert.AreEqual(1, taskMap.Count);
            Assert.AreSame(nodes[0], taskMap.Keys.Single());

            Assert.AreEqual(PlatformTestNodeRunner + "123", await res.GetResultAsync());
        }

        [Test]
        public async Task TestBroadcastAllNodes()
        {
            var nodes = await Client.GetClusterNodesAsync();

            IDictionary<IClusterNode, Task<IJobExecution<string>>> taskMap = Client.Compute.SubmitBroadcast<string>(
                nodes,
                Units,
                NodeNameJob,
                JobExecutionOptions.Default,
                "123");
            var res1 = await taskMap[nodes[0]];
            var res2 = await taskMap[nodes[1]];
            var res3 = await taskMap[nodes[2]];
            var res4 = await taskMap[nodes[3]];

            Assert.AreEqual(4, taskMap.Count);

            Assert.AreEqual(nodes[0].Name + "123", await res1.GetResultAsync());
            Assert.AreEqual(nodes[1].Name + "123", await res2.GetResultAsync());
            Assert.AreEqual(nodes[2].Name + "123", await res3.GetResultAsync());
            Assert.AreEqual(nodes[3].Name + "123", await res4.GetResultAsync());
        }

        [Test]
        public async Task TestExecuteWithArgs()
        {
            var res = await Client.Compute.SubmitAsync<string>(
                await Client.GetClusterNodesAsync(),
                Units,
                ConcatJob,
                JobExecutionOptions.Default,
                1.1,
                Guid.Empty,
                "3",
                null);

            Assert.AreEqual("1.1_00000000-0000-0000-0000-000000000000_3_null", await res.GetResultAsync());
        }

        [Test]
        public async Task TestExecuteWithNullArgs()
        {
            var res = await Client.Compute.SubmitAsync<string>(await Client.GetClusterNodesAsync(), Units, ConcatJob, args: null);

            Assert.IsNull(await res.GetResultAsync());
        }

        [Test]
        public async Task TestJobErrorPropagatesToClientWithClassAndMessage()
        {
            var jobExecution = await Client.Compute.SubmitAsync<string>(await Client.GetClusterNodesAsync(), Units, ErrorJob, "unused");
            var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExecution.GetResultAsync());

            StringAssert.Contains("Custom job error", ex!.Message);

            StringAssert.StartsWith(
                "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest$CustomException",
                ex.InnerException!.Message);

            Assert.AreEqual(ErrorGroups.Table.ColumnAlreadyExists, ex.Code);
            Assert.AreEqual("IGN-TBL-3", ex.CodeAsString);
            Assert.AreEqual(3, ex.ErrorCode);
            Assert.AreEqual("TBL", ex.GroupName);
        }

        [Test]
        public void TestUnknownNodeSubmitThrows()
        {
            var unknownNode = new ClusterNode("x", "y", new IPEndPoint(IPAddress.Loopback, 0));

            var ex = Assert.ThrowsAsync<NodeNotFoundException>(async () =>
                await Client.Compute.SubmitAsync<string>(new[] { unknownNode }, Units, EchoJob, "unused"));

            StringAssert.Contains("None of the specified nodes are present in the cluster: [y]", ex!.Message);
            Assert.AreEqual(ErrorGroups.Compute.NodeNotFound, ex.Code);
        }

        [Test]
        public void TestUnknownNodeSubmitBroadcastThrows()
        {
            var unknownNode = new ClusterNode("x", "y", new IPEndPoint(IPAddress.Loopback, 0));

            IDictionary<IClusterNode, Task<IJobExecution<string>>> taskMap =
                Client.Compute.SubmitBroadcast<string>(new[] { unknownNode }, Units, EchoJob, "unused");

            var ex = Assert.ThrowsAsync<NodeNotFoundException>(async () => await taskMap[unknownNode]);

            StringAssert.Contains("None of the specified nodes are present in the cluster: [y]", ex!.Message);
            Assert.AreEqual(ErrorGroups.Compute.NodeNotFound, ex.Code);
        }

        [Test]
        public async Task TestAllSupportedArgTypes()
        {
            await Test(sbyte.MinValue);
            await Test(sbyte.MaxValue);
            await Test(short.MinValue);
            await Test(short.MaxValue);
            await Test(int.MinValue);
            await Test(int.MaxValue);
            await Test(long.MinValue);
            await Test(long.MaxValue);
            await Test(float.MinValue);
            await Test(float.MaxValue);
            await Test(double.MinValue);
            await Test(double.MaxValue);

            await Test(123.456m);
            await Test(-123.456m);
            await Test(decimal.MinValue);
            await Test(decimal.MaxValue);

            await Test(new byte[] { 1, 255 });
            await Test("Ignite 🔥");
            await Test(new BitArray(new[] { byte.MaxValue }), "{0, 1, 2, 3, 4, 5, 6, 7}");
            await Test(LocalDate.MinIsoValue, "-9998-01-01");
            await Test(LocalTime.Noon, "12:00");
            await Test(LocalDateTime.MaxIsoValue, "9999-12-31T23:59:59.999999999");
            await Test(Instant.FromUtc(2001, 3, 4, 5, 6));

            await Test(BigInteger.One);
            await Test(BigInteger.Zero);
            await Test(BigInteger.MinusOne);
            await Test(new BigInteger(123456));
            await Test(BigInteger.Pow(1234, 56));

            await Test(Guid.Empty);
            await Test(new Guid(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }));
            await Test(Guid.NewGuid());

            async Task Test(object val, string? expectedStr = null)
            {
                var nodes = await Client.GetClusterNodesAsync();
                var str = expectedStr ?? val.ToString()!.Replace("E+", "E");
                IJobExecution<object> resExec = await Client.Compute.SubmitAsync<object>(nodes, Units, EchoJob, val, str);
                object res = await resExec.GetResultAsync();

                Assert.AreEqual(val, res);
            }
        }

        [Test]
        [TestCase(1, 2)]
        [TestCase(2, 1)]
        [TestCase(4, 3)]
        [TestCase(5, 4)]
        [TestCase(6, 3)]
        [TestCase(7, 1)]
        [TestCase(8, 2)]
        [TestCase(9, 1)]
        [TestCase(10, 2)]
        [TestCase(11, 4)]
        public async Task TestExecuteColocated(long key, int nodeIdx)
        {
            var proxies = GetProxies();
            using var client = await IgniteClient.StartAsync(GetConfig(proxies));
            client.WaitForConnections(proxies.Count);

            var keyTuple = new IgniteTuple { [KeyCol] = key };
            var resNodeName = await client.Compute.SubmitColocatedAsync<string>(TableName, keyTuple, Units, NodeNameJob);
            var requestTargetNodeName = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var keyPoco = new Poco { Key = key };
            var resNodeName2 = await client.Compute.SubmitColocatedAsync<string, Poco>(TableName, keyPoco, Units.Reverse(), NodeNameJob);
            var requestTargetNodeName2 = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var keyPocoStruct = new PocoStruct(key, null);
            var resNodeName3 = await client.Compute.SubmitColocatedAsync<string, PocoStruct>(TableName, keyPocoStruct, Units, NodeNameJob);
            var requestTargetNodeName3 = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var nodeName = nodeIdx == 1 ? string.Empty : "_" + nodeIdx;
            var expectedNodeName = PlatformTestNodeRunner + nodeName;

            Assert.AreEqual(expectedNodeName, await resNodeName.GetResultAsync());
            Assert.AreEqual(expectedNodeName, await resNodeName2.GetResultAsync());
            Assert.AreEqual(expectedNodeName, await resNodeName3.GetResultAsync());

            // We only connect to 2 of 4 nodes because of different auth settings.
            if (nodeIdx < 3)
            {
                Assert.AreEqual(expectedNodeName, requestTargetNodeName);
                Assert.AreEqual(expectedNodeName, requestTargetNodeName2);
                Assert.AreEqual(expectedNodeName, requestTargetNodeName3);
            }
        }

        [Test]
        public void TestExecuteColocatedThrowsWhenTableDoesNotExist()
        {
            var ex = Assert.ThrowsAsync<IgniteClientException>(async () =>
                await Client.Compute.SubmitColocatedAsync<string>("unknownTable", new IgniteTuple(), Units, EchoJob));

            Assert.AreEqual("Table 'unknownTable' does not exist.", ex!.Message);
        }

        [Test]
        public void TestExecuteColocatedThrowsWhenKeyColumnIsMissing()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
                await Client.Compute.SubmitColocatedAsync<string>(TableName, new IgniteTuple { ["VAL"] = "1" }, Units, EchoJob));

            Assert.AreEqual(
                "Can't map 'IgniteTuple { VAL = 1 }' to columns 'Int64 KEY, String VAL'. Matching fields not found.",
                ex!.Message);
        }

        [Test]
        public async Task TestExecuteColocatedUpdatesTableCacheOnTableDrop([Values(false, true)] bool forceLoadAssignment)
        {
            // Create table and use it in ExecuteColocated.
            var nodes = await GetNodeAsync(0);
            var tableNameExec = await Client.Compute.SubmitAsync<string>(nodes, Units, CreateTableJob, "drop_me");
            var tableName = await tableNameExec.GetResultAsync();

            try
            {
                var keyTuple = new IgniteTuple { [KeyCol] = 1L };
                var resNodeNameExec = await Client.Compute.SubmitColocatedAsync<string>(tableName, keyTuple, Units, NodeNameJob);
                var resNodeName = await resNodeNameExec.GetResultAsync();

                // Drop table and create a new one with a different ID, then execute a computation again.
                // This should update the cached table and complete the computation successfully.
                var dropExec = await Client.Compute.SubmitAsync<string>(nodes, Units, DropTableJob, tableName);
                await dropExec.GetResultAsync();

                var createExec = await Client.Compute.SubmitAsync<string>(nodes, Units, CreateTableJob, tableName);
                await createExec.GetResultAsync();

                if (forceLoadAssignment)
                {
                    var table = Client.Compute.GetFieldValue<IDictionary>("_tableCache")[tableName]!;
                    table.SetFieldValue("_partitionAssignment", null);
                }

                var resNodeName2Exec = await Client.Compute.SubmitColocatedAsync<string>(tableName, keyTuple, Units, NodeNameJob);
                var resNodeName2 = await resNodeName2Exec.GetResultAsync();

                Assert.AreEqual(resNodeName, resNodeName2);
            }
            finally
            {
                await (await Client.Compute.SubmitAsync<string>(nodes, Units, DropTableJob, tableName)).GetResultAsync();
            }
        }

        [Test]
        public async Task TestExecuteColocatedWithEscapedTableName()
        {
            var tableName = "\"table with spaces\"";
            var key = 1L;
            var tupleKey = new IgniteTuple { ["KEY"] = key };

            try
            {
                await Client.Sql.ExecuteAsync(null, $"CREATE TABLE {tableName} (KEY BIGINT PRIMARY KEY, VAL VARCHAR)");

                var exec1 = await Client.Compute.SubmitColocatedAsync<string>(tableName, tupleKey, Units, NodeNameJob);
                var exec2 = await Client.Compute.SubmitColocatedAsync<string, long>(tableName, key, Units, NodeNameJob);

                var res1 = await exec1.GetResultAsync();
                var res2 = await exec2.GetResultAsync();

                Assert.AreEqual(res1, res2);
            }
            finally
            {
                await Client.Sql.ExecuteAsync(null, $"DROP TABLE IF EXISTS {tableName}");
            }
        }

        [Test]
        public async Task TestExceptionInJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTrace()
        {
            var jobExecution = await Client.Compute.SubmitAsync<object>(await GetNodeAsync(1), Units, ExceptionJob, "foo-bar");
            var ex = Assert.ThrowsAsync<ComputeException>(async () => await jobExecution.GetResultAsync());

            Assert.AreEqual("Job execution failed: java.lang.RuntimeException: Test exception: foo-bar", ex!.Message);
            Assert.IsNotNull(ex.InnerException);

            var str = ex.ToString();

            StringAssert.Contains(
                "at org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$ExceptionJob.execute(PlatformTestNodeRunner.java:",
                str);
        }

        [Test]
        public async Task TestCheckedExceptionInJobPropagatesToClient()
        {
            var jobExecution = await Client.Compute.SubmitAsync<object>(await GetNodeAsync(1), Units, CheckedExceptionJob, "foo-bar");
            var ex = Assert.ThrowsAsync<IgniteException>(async () => await jobExecution.GetResultAsync());

            Assert.AreEqual("TestCheckedEx: foo-bar", ex!.Message);
            Assert.IsNotNull(ex.InnerException);

            StringAssert.Contains("org.apache.ignite.lang.IgniteCheckedException: IGN-CMN-5", ex.ToString());
        }

        [Test]
        public async Task TestDeploymentUnitsPropagation()
        {
            var units = new DeploymentUnit[]
            {
                new("unit-latest"),
                new("unit1", "1.0.0")
            };

            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var res = await client.Compute.SubmitAsync<string>(await GetNodeAsync(1), units, FakeServer.GetDetailsJob);
            StringAssert.Contains("Units = unit-latest|latest, unit1|1.0.0", await res.GetResultAsync());

            // Lazy enumerable.
            var res2 = await client.Compute.SubmitAsync<string>(await GetNodeAsync(1), units.Reverse(), FakeServer.GetDetailsJob);
            StringAssert.Contains("Units = unit1|1.0.0, unit-latest|latest", await res2.GetResultAsync());

            // Colocated.
            var keyTuple = new IgniteTuple { ["ID"] = 1 };
            var res3 = await client.Compute.SubmitColocatedAsync<string>(
                FakeServer.ExistingTableName, keyTuple, units, FakeServer.GetDetailsJob);

            StringAssert.Contains("Units = unit-latest|latest, unit1|1.0.0", await res3.GetResultAsync());
        }

        [Test]
        public void TestExecuteOnUnknownUnitWithLatestVersionThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new("unit-latest") };

            var ex = Assert.ThrowsAsync<IgniteException>(
                async () => await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            StringAssert.Contains("Deployment unit unit-latest:latest doesn't exist", ex!.Message);
        }

        [Test]
        public void TestExecuteColocatedOnUnknownUnitWithLatestVersionThrows()
        {
            var keyTuple = new IgniteTuple { [KeyCol] = 1L };
            var deploymentUnits = new DeploymentUnit[] { new("unit-latest") };

            var ex = Assert.ThrowsAsync<IgniteException>(
                async () => await Client.Compute.SubmitColocatedAsync<string>(TableName, keyTuple, deploymentUnits, NodeNameJob));

            StringAssert.Contains("Deployment unit unit-latest:latest doesn't exist", ex!.Message);
        }

        [Test]
        public void TestNullUnitNameThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new(null!) };

            var ex = Assert.ThrowsAsync<ArgumentNullException>(
                async () => await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("Value cannot be null. (Parameter 'unit.Name')", ex!.Message);
        }

        [Test]
        public void TestEmptyUnitNameThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new(string.Empty) };

            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("The value cannot be an empty string. (Parameter 'unit.Name')", ex!.Message);
        }

        [Test]
        public void TestNullUnitVersionThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new("u", null!) };

            var ex = Assert.ThrowsAsync<ArgumentNullException>(
                async () => await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("Value cannot be null. (Parameter 'unit.Version')", ex!.Message);
        }

        [Test]
        public void TestEmptyUnitVersionThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new("u", string.Empty) };

            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("The value cannot be an empty string. (Parameter 'unit.Version')", ex!.Message);
        }

        [Test]
        public async Task TestDelayedJobExecutionThrowsWhenConnectionFails()
        {
            // Compute jobs are completed with a notification message.
            // In this test, the job execution starts, but we drop the client connection, so notification can't be received.
            using var client = await IgniteClient.StartAsync(GetConfig());

            const int sleepMs = 3000;
            var jobExecution = await client.Compute.SubmitAsync<string>(await GetNodeAsync(1), Units, SleepJob, sleepMs);
            var jobTask = jobExecution.GetResultAsync();

            // Wait a bit and close the connection.
            await Task.Delay(10);
            client.Dispose();

            var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await jobTask);
            Assert.AreEqual("Connection closed.", ex!.Message);
        }

        [Test]
        public async Task TestJobExecutionStatusExecuting()
        {
            const int sleepMs = 3000;
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), Units, SleepJob, sleepMs);

            await AssertJobStatus(jobExecution, JobState.Executing, beforeStart);
        }

        [Test]
        public async Task TestJobExecutionStatusCompleted()
        {
            const int sleepMs = 1;
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), Units, SleepJob, sleepMs);
            await jobExecution.GetResultAsync();

            await AssertJobStatus(jobExecution, JobState.Completed, beforeStart);
        }

        [Test]
        public async Task TestJobExecutionStatusFailed()
        {
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), Units, ErrorJob, "unused");
            Assert.CatchAsync(async () => await jobExecution.GetResultAsync());

            await AssertJobStatus(jobExecution, JobState.Failed, beforeStart);
        }

        [Test]
        public async Task TestJobExecutionStatusNull()
        {
            var fakeJobExecution = new JobExecution<int>(
                Guid.NewGuid(), Task.FromException<(int, JobStatus)>(new Exception("x")), (Compute)Client.Compute);

            var status = await fakeJobExecution.GetStatusAsync();

            Assert.IsNull(status);
        }

        [Test]
        public async Task TestJobExecutionCancel()
        {
            const int sleepMs = 5000;
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync<string>(await GetNodeAsync(1), Units, SleepJob, sleepMs);
            await jobExecution.CancelAsync();

            await AssertJobStatus(jobExecution, JobState.Canceled, beforeStart);
        }

        [Test]
        public async Task TestChangePriority()
        {
            var jobExecution = await Client.Compute.SubmitAsync<string>(
                await GetNodeAsync(1),
                Units,
                SleepJob,
                new JobExecutionOptions(Priority: 10, MaxRetries: 11),
                5000);
            var res = await jobExecution.ChangePriorityAsync(123);

            // Job exists, but is already executing.
            Assert.IsFalse(res);
        }

        [Test]
        public async Task TestJobExecutionOptionsPropagation()
        {
            // ReSharper disable once WithExpressionModifiesAllMembers
            var options = JobExecutionOptions.Default with { Priority = 999, MaxRetries = 66 };
            var units = new DeploymentUnit[] { new("unit1", "1.0.0") };

            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var defaultRes = await client.Compute.SubmitAsync<string>(
                await GetNodeAsync(1), units, FakeServer.GetDetailsJob, JobExecutionOptions.Default);
            StringAssert.Contains("priority = 0, maxRetries = 0", await defaultRes.GetResultAsync());

            var res = await client.Compute.SubmitAsync<string>(await GetNodeAsync(1), units, FakeServer.GetDetailsJob, options);
            StringAssert.Contains("priority = 999, maxRetries = 66", await res.GetResultAsync());

            // Colocated.
            var keyTuple = new IgniteTuple { ["ID"] = 1 };
            var colocatedRes = await client.Compute.SubmitColocatedAsync<string>(
                FakeServer.ExistingTableName, keyTuple, units, FakeServer.GetDetailsJob, options);

            StringAssert.Contains("priority = 999, maxRetries = 66", await colocatedRes.GetResultAsync());
        }

        [Test]
        [TestCase("1E3", -3)]
        [TestCase("1.12E5", 0)]
        [TestCase("1.123456789", 10)]
        [TestCase("1.123456789", 5)]
        public async Task TestBigDecimalPropagation(string number, int scale)
        {
            var res = await Client.Compute.SubmitAsync<decimal>(await GetNodeAsync(1), Units, DecimalJob, number, scale);
            var resVal = await res.GetResultAsync();

            var expected = decimal.Parse(number, NumberStyles.Float);

            if (scale > 0)
            {
                expected = decimal.Round(expected, scale);
            }

            Assert.AreEqual(expected, resVal);
        }

        private static async Task AssertJobStatus<T>(IJobExecution<T> jobExecution, JobState state, Instant beforeStart)
        {
            JobStatus? status = await jobExecution.GetStatusAsync();

            Assert.IsNotNull(status);
            Assert.AreEqual(jobExecution.Id, status!.Id);
            Assert.AreEqual(state, status.State);
            Assert.Greater(status.CreateTime, beforeStart);
            Assert.Greater(status.StartTime, status.CreateTime);

            if (state is JobState.Canceled or JobState.Completed or JobState.Failed)
            {
                Assert.Greater(status.FinishTime, status.StartTime);
            }
            else
            {
                Assert.IsNull(status.FinishTime);
            }
        }

        private async Task<List<IClusterNode>> GetNodeAsync(int index) =>
            (await Client.GetClusterNodesAsync()).OrderBy(n => n.Name).Skip(index).Take(1).ToList();
    }
}
