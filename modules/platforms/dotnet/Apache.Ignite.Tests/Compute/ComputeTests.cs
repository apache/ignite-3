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
    using System.Threading.Tasks;
    using Ignite.Compute;
    using Ignite.Marshalling;
    using Ignite.Table;
    using Internal.Compute;
    using Internal.Network;
    using Internal.Proto;
    using Network;
    using NodaTime;
    using NUnit.Framework;
    using Table;
    using TaskStatus = Ignite.Compute.TaskStatus;

    /// <summary>
    /// Tests <see cref="ICompute"/>.
    /// </summary>
    public class ComputeTests : IgniteTestsBase
    {
        public const string PlatformTestNodeRunner = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner";

        public const string ItThinClientComputeTest = "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest";

        public static readonly JobDescriptor<object?, string> NodeNameJob = new(ItThinClientComputeTest + "$NodeNameJob");

        public static readonly JobDescriptor<string?, string> ConcatJob = new(ItThinClientComputeTest + "$ConcatJob");

        public static readonly JobDescriptor<string, string> ErrorJob = new(ItThinClientComputeTest + "$IgniteExceptionJob");

        public static readonly JobDescriptor<object?, object> EchoJob = new(ItThinClientComputeTest + "$EchoJob");

        public static readonly JobDescriptor<object, string> ToStringJob = new(ItThinClientComputeTest + "$ToStringJob");

        public static readonly JobDescriptor<int, string> SleepJob = new(ItThinClientComputeTest + "$SleepJob");

        public static readonly JobDescriptor<string, decimal> DecimalJob = new(ItThinClientComputeTest + "$DecimalJob");

        public static readonly JobDescriptor<string, string> CreateTableJob = new(PlatformTestNodeRunner + "$CreateTableJob");

        public static readonly JobDescriptor<string, string> DropTableJob = new(PlatformTestNodeRunner + "$DropTableJob");

        public static readonly JobDescriptor<object, object> ExceptionJob = new(PlatformTestNodeRunner + "$ExceptionJob");

        public static readonly JobDescriptor<string, object> CheckedExceptionJob = new(PlatformTestNodeRunner + "$CheckedExceptionJob");

        public static readonly JobDescriptor<long, int> PartitionJob = new(PlatformTestNodeRunner + "$PartitionJob");

        public static readonly TaskDescriptor<string, string> NodeNameTask = new(ItThinClientComputeTest + "$MapReduceNodeNameTask");

        public static readonly TaskDescriptor<int, object?> SleepTask = new(PlatformTestNodeRunner + "$SleepTask");

        public static readonly TaskDescriptor<object?, object?> SplitExceptionTask = new(ItThinClientComputeTest + "$MapReduceExceptionOnSplitTask");

        public static readonly TaskDescriptor<object?, object?> ReduceExceptionTask = new(ItThinClientComputeTest + "$MapReduceExceptionOnReduceTask");

        [Test]
        public async Task TestGetClusterNodes()
        {
            var res = (await Client.GetClusterNodesAsync()).OrderBy(x => x.Name).ToList();

            Assert.AreEqual(4, res.Count);

            Assert.IsNotEmpty(res[0].Id);
            Assert.IsNotEmpty(res[1].Id);

            var addrs = res.Select(x => (IPEndPoint)x.Address).ToArray();

            Assert.AreEqual(3344, addrs[0].Port);
            Assert.AreEqual(3345, addrs[1].Port);
            Assert.AreEqual(addrs[0].Address, addrs[1].Address);
        }

        [Test]
        public async Task TestExecuteOnSpecificNode()
        {
            var res1 = await Client.Compute.SubmitAsync(await GetNodeAsync(0), NodeNameJob, "-11");
            var res2 = await Client.Compute.SubmitAsync(await GetNodeAsync(1), NodeNameJob, 33);

            Assert.AreEqual(PlatformTestNodeRunner + "-11", await res1.GetResultAsync());
            Assert.AreEqual(PlatformTestNodeRunner + "_233", await res2.GetResultAsync());
        }

        [Test]
        public async Task TestExecuteOnRandomNode()
        {
            var jobTarget = JobTarget.AnyNode(await Client.GetClusterNodesAsync());
            var jobExecution = await Client.Compute.SubmitAsync(jobTarget, NodeNameJob, null);
            var res = await jobExecution.GetResultAsync();

            var expectedNodeNames = Enumerable.Range(1, 4)
                .Select(x => x == 1 ? PlatformTestNodeRunner : PlatformTestNodeRunner + "_" + x)
                .ToList();

            CollectionAssert.Contains(expectedNodeNames, res);
        }

        [Test]
        public async Task TestExecuteResultTypeMismatchThrowsInvalidCastException()
        {
            var jobDescriptor = new JobDescriptor<object?, Guid>(NodeNameJob.JobClassName);
            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(0), jobDescriptor, null);
            Assert.ThrowsAsync<InvalidCastException>(async () => await jobExecution.GetResultAsync());
        }

        [Test]
        public async Task TestBroadcastOneNode()
        {
            var node = (await GetNodeAsync(0)).Data;

            IDictionary<IClusterNode, Task<IJobExecution<string>>> taskMap = Client.Compute.SubmitBroadcast(
                new[] { node },
                NodeNameJob,
                "123");

            var res = await taskMap[node];

            Assert.AreEqual(1, taskMap.Count);
            Assert.AreSame(node, taskMap.Keys.Single());

            Assert.AreEqual(PlatformTestNodeRunner + "123", await res.GetResultAsync());
        }

        [Test]
        public async Task TestBroadcastAllNodes()
        {
            var nodes = await Client.GetClusterNodesAsync();

            IDictionary<IClusterNode, Task<IJobExecution<string>>> taskMap = Client.Compute.SubmitBroadcast(
                nodes,
                NodeNameJob,
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
        public async Task TestExecuteWithNullArgs()
        {
            var res = await Client.Compute.SubmitAsync(await GetNodeAsync(0), ConcatJob, null);

            Assert.IsNull(await res.GetResultAsync());
        }

        [Test]
        public async Task TestJobErrorPropagatesToClientWithClassAndMessage()
        {
            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(0), ErrorJob, "unused");
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
            var unknownNode = JobTarget.Node(new ClusterNode("x", "y", new IPEndPoint(IPAddress.Loopback, 0)));

            var ex = Assert.ThrowsAsync<NodeNotFoundException>(async () =>
                await Client.Compute.SubmitAsync(unknownNode, EchoJob, "unused"));

            StringAssert.Contains("None of the specified nodes are present in the cluster: [y]", ex!.Message);
            Assert.AreEqual(ErrorGroups.Compute.NodeNotFound, ex.Code);
        }

        [Test]
        public void TestUnknownNodeSubmitBroadcastThrows()
        {
            var unknownNode = new ClusterNode("x", "y", new IPEndPoint(IPAddress.Loopback, 0));

            IDictionary<IClusterNode, Task<IJobExecution<object>>> taskMap =
                Client.Compute.SubmitBroadcast(new[] { unknownNode }, EchoJob, "unused");

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

            await Test(new byte[] { 1, 255 }, "[1, -1]");
            await Test("Ignite 🔥");
            await Test(LocalDate.MinIsoValue, "-9998-01-01");
            await Test(LocalTime.Noon, "12:00");
            await Test(LocalDateTime.MaxIsoValue, "9999-12-31T23:59:59.999999999");
            await Test(Instant.FromUtc(2001, 3, 4, 5, 6));

            await Test(Guid.Empty);
            await Test(new Guid(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }));
            await Test(Guid.NewGuid());

            async Task Test(object val, string? expectedStr = null)
            {
                var nodes = JobTarget.AnyNode(await Client.GetClusterNodesAsync());

                IJobExecution<object> resExec = await Client.Compute.SubmitAsync(nodes, EchoJob, val);
                object res = await resExec.GetResultAsync();

                Assert.AreEqual(val, res);

                var strExec = await Client.Compute.SubmitAsync(nodes, ToStringJob, val);
                var str = await strExec.GetResultAsync();

                var expectedStr0 = expectedStr ?? val.ToString()!.Replace("E+", "E");
                Assert.AreEqual(expectedStr0, str);
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
            var resNodeName = await client.Compute.SubmitAsync(JobTarget.Colocated(TableName, keyTuple), NodeNameJob, null);
            var requestTargetNodeName = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var keyPoco = new Poco { Key = key };
            var resNodeName2 = await client.Compute.SubmitAsync(JobTarget.Colocated(TableName, keyPoco), NodeNameJob, null);
            var requestTargetNodeName2 = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var keyPocoStruct = new PocoStruct(key, null);
            var resNodeName3 = await client.Compute.SubmitAsync(JobTarget.Colocated(TableName, keyPocoStruct), NodeNameJob, null);
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
                await Client.Compute.SubmitAsync(JobTarget.Colocated("unknownTable", new IgniteTuple()), EchoJob, null));

            Assert.AreEqual("Table 'unknownTable' does not exist.", ex!.Message);
        }

        [Test]
        public void TestExecuteColocatedThrowsWhenKeyColumnIsMissing()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
                await Client.Compute.SubmitAsync(JobTarget.Colocated(TableName, new IgniteTuple { ["VAL"] = "1" }), EchoJob, null));

            Assert.AreEqual(
                "Can't map 'IgniteTuple { VAL = 1 }' to columns 'Int64 KEY, String VAL'. Matching fields not found.",
                ex!.Message);
        }

        [Test]
        public async Task TestExecuteColocatedUpdatesTableCacheOnTableDrop([Values(false, true)] bool forceLoadAssignment)
        {
            // Create table and use it in ExecuteColocated.
            var nodes = await GetNodeAsync(0);
            var tableNameExec = await Client.Compute.SubmitAsync(nodes, CreateTableJob, "drop_me");
            var tableName = await tableNameExec.GetResultAsync();

            try
            {
                var keyTuple = new IgniteTuple { [KeyCol] = 1L };
                var resNodeNameExec = await Client.Compute.SubmitAsync(JobTarget.Colocated(tableName, keyTuple), NodeNameJob, null);
                var resNodeName = await resNodeNameExec.GetResultAsync();

                // Drop table and create a new one with a different ID, then execute a computation again.
                // This should update the cached table and complete the computation successfully.
                var dropExec = await Client.Compute.SubmitAsync(nodes, DropTableJob, tableName);
                await dropExec.GetResultAsync();

                var createExec = await Client.Compute.SubmitAsync(nodes, CreateTableJob, tableName);
                await createExec.GetResultAsync();

                if (forceLoadAssignment)
                {
                    var table = Client.Compute.GetFieldValue<IDictionary>("_tableCache")[tableName]!;
                    table.SetFieldValue("_partitionAssignment", null);
                }

                var resNodeName2Exec = await Client.Compute.SubmitAsync(JobTarget.Colocated(tableName, keyTuple), NodeNameJob, null);
                var resNodeName2 = await resNodeName2Exec.GetResultAsync();

                Assert.AreEqual(resNodeName, resNodeName2);
            }
            finally
            {
                await (await Client.Compute.SubmitAsync(nodes, DropTableJob, tableName)).GetResultAsync();
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

                var exec1 = await Client.Compute.SubmitAsync(JobTarget.Colocated(tableName, tupleKey), NodeNameJob, null);
                var exec2 = await Client.Compute.SubmitAsync(JobTarget.Colocated(tableName, key), NodeNameJob, null);

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
            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(1), ExceptionJob, "foo-bar");
            var ex = Assert.ThrowsAsync<ComputeException>(async () => await jobExecution.GetResultAsync());

            Assert.AreEqual("Job execution failed: java.lang.RuntimeException: Test exception: foo-bar", ex!.Message);
            Assert.IsNotNull(ex.InnerException);

            var str = ex.ToString();

            StringAssert.Contains(
                "at org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$ExceptionJob.executeAsync(PlatformTestNodeRunner.java:",
                str);
        }

        [Test]
        public async Task TestCheckedExceptionInJobPropagatesToClient()
        {
            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(1), CheckedExceptionJob, "foo-bar");
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

            var res = await client.Compute.SubmitAsync(
                await GetNodeAsync(1),
                new JobDescriptor<object?, string>(FakeServer.GetDetailsJob, units),
                null);

            StringAssert.Contains("Units = unit-latest|latest, unit1|1.0.0", await res.GetResultAsync());

            // Lazy enumerable.
            var res2 = await client.Compute.SubmitAsync(
                await GetNodeAsync(1),
                new JobDescriptor<object?, string>(FakeServer.GetDetailsJob, units.Reverse()),
                null);

            StringAssert.Contains("Units = unit1|1.0.0, unit-latest|latest", await res2.GetResultAsync());

            // Colocated.
            var keyTuple = new IgniteTuple { ["ID"] = 1 };
            var res3 = await client.Compute.SubmitAsync(
                JobTarget.Colocated(FakeServer.ExistingTableName, keyTuple),
                new JobDescriptor<object?, string>(FakeServer.GetDetailsJob, units),
                null);

            StringAssert.Contains("Units = unit-latest|latest, unit1|1.0.0", await res3.GetResultAsync());
        }

        [Test]
        public void TestExecuteOnUnknownUnitWithLatestVersionThrows()
        {
            var job = NodeNameJob with
            {
                DeploymentUnits = new DeploymentUnit[]
                {
                    new("unit-latest")
                }
            };

            var ex = Assert.ThrowsAsync<IgniteException>(
                async () => await Client.Compute.SubmitAsync(await GetNodeAsync(1), job, null));

            StringAssert.Contains("Deployment unit unit-latest:latest doesn't exist", ex!.Message);
        }

        [Test]
        public void TestExecuteColocatedOnUnknownUnitWithLatestVersionThrows()
        {
            var keyTuple = new IgniteTuple { [KeyCol] = 1L };
            var job = NodeNameJob with
            {
                DeploymentUnits = new DeploymentUnit[]
                {
                    new("unit-latest")
                }
            };

            var ex = Assert.ThrowsAsync<IgniteException>(
                async () => await Client.Compute.SubmitAsync(JobTarget.Colocated(TableName, keyTuple), job, null));

            StringAssert.Contains("Deployment unit unit-latest:latest doesn't exist", ex!.Message);
        }

        [Test]
        public void TestNullUnitNameThrows()
        {
            var job = NodeNameJob with
            {
                DeploymentUnits = new DeploymentUnit[]
                {
                    new(null!)
                }
            };

            var ex = Assert.ThrowsAsync<ArgumentNullException>(
                async () => await Client.Compute.SubmitAsync(await GetNodeAsync(1), job, null));

            Assert.AreEqual("Value cannot be null. (Parameter 'unit.Name')", ex!.Message);
        }

        [Test]
        public void TestEmptyUnitNameThrows()
        {
            var job = NodeNameJob with
            {
                DeploymentUnits = new DeploymentUnit[]
                {
                    new(string.Empty)
                }
            };

            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Compute.SubmitAsync(await GetNodeAsync(1), job, null));

            Assert.AreEqual("The value cannot be an empty string. (Parameter 'unit.Name')", ex!.Message);
        }

        [Test]
        public void TestNullUnitVersionThrows()
        {
            var job = NodeNameJob with
            {
                DeploymentUnits = new DeploymentUnit[]
                {
                    new("u", null!)
                }
            };

            var ex = Assert.ThrowsAsync<ArgumentNullException>(
                async () => await Client.Compute.SubmitAsync(await GetNodeAsync(1), job, null));

            Assert.AreEqual("Value cannot be null. (Parameter 'unit.Version')", ex!.Message);
        }

        [Test]
        public void TestEmptyUnitVersionThrows()
        {
            var job = NodeNameJob with
            {
                DeploymentUnits = new DeploymentUnit[]
                {
                    new("u", string.Empty)
                }
            };

            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Compute.SubmitAsync(await GetNodeAsync(1), job, null));

            Assert.AreEqual("The value cannot be an empty string. (Parameter 'unit.Version')", ex!.Message);
        }

        [Test]
        public async Task TestDelayedJobExecutionThrowsWhenConnectionFails()
        {
            // Compute jobs are completed with a notification message.
            // In this test, the job execution starts, but we drop the client connection, so notification can't be received.
            using var client = await IgniteClient.StartAsync(GetConfig());

            const int sleepMs = 3000;
            var jobExecution = await client.Compute.SubmitAsync(await GetNodeAsync(1), SleepJob, sleepMs);
            var jobTask = jobExecution.GetResultAsync();

            // Wait a bit and close the connection.
            await Task.Delay(10);

            // ReSharper disable once DisposeOnUsingVariable (intentional)
            client.Dispose();

            var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await jobTask);
            Assert.AreEqual("Connection closed.", ex!.Message);
        }

        [Test]
        public async Task TestJobExecutionStatusExecuting()
        {
            const int sleepMs = 3000;
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(1), SleepJob, sleepMs);

            await AssertJobStatus(jobExecution, JobStatus.Executing, beforeStart);
        }

        [Test]
        public async Task TestJobExecutionStatusCompleted()
        {
            const int sleepMs = 1;
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(1), SleepJob, sleepMs);
            await jobExecution.GetResultAsync();

            await AssertJobStatus(jobExecution, JobStatus.Completed, beforeStart);
        }

        [Test]
        public async Task TestJobExecutionStatusFailed()
        {
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(1), ErrorJob, "unused");
            Assert.CatchAsync(async () => await jobExecution.GetResultAsync());

            await AssertJobStatus(jobExecution, JobStatus.Failed, beforeStart);
        }

        [Test]
        public async Task TestJobExecutionStatusNull()
        {
            var fakeJobExecution = new JobExecution<int>(
                Guid.NewGuid(), Task.FromException<(int, JobState)>(new Exception("x")), (Compute)Client.Compute);

            var status = await fakeJobExecution.GetStateAsync();

            Assert.IsNull(status);
        }

        [Test]
        public async Task TestJobExecutionCancel()
        {
            const int sleepMs = 5000;
            var beforeStart = SystemClock.Instance.GetCurrentInstant();

            var jobExecution = await Client.Compute.SubmitAsync(await GetNodeAsync(1), SleepJob, sleepMs);
            await jobExecution.CancelAsync();

            await AssertJobStatus(jobExecution, JobStatus.Canceled, beforeStart);
        }

        [Test]
        public async Task TestChangeJobPriority()
        {
            var jobExecution = await Client.Compute.SubmitAsync(
                await GetNodeAsync(1),
                SleepJob with
                {
                    Options = new(Priority: 10, MaxRetries: 11)
                },
                5000);

            var res = await jobExecution.ChangePriorityAsync(123);

            // Job exists, but is already executing.
            Assert.IsFalse(res);
        }

        [Test]
        public async Task TestJobExecutionOptionsPropagation()
        {
            // ReSharper disable once WithExpressionModifiesAllMembers
            var job = new JobDescriptor<object?, string>(FakeServer.GetDetailsJob)
            {
                Options = JobExecutionOptions.Default with
                {
                    Priority = 999,
                    MaxRetries = 66
                },
                DeploymentUnits = new DeploymentUnit[] { new("unit1", "1.0.0") }
            };

            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var defaultRes = await client.Compute.SubmitAsync(await GetNodeAsync(1), job with { Options = default }, null);
            StringAssert.Contains("priority = 0, maxRetries = 0", await defaultRes.GetResultAsync());

            var res = await client.Compute.SubmitAsync(await GetNodeAsync(1), job, null);
            StringAssert.Contains("priority = 999, maxRetries = 66", await res.GetResultAsync());

            // Colocated.
            var keyTuple = new IgniteTuple { ["ID"] = 1 };
            var colocatedRes = await client.Compute.SubmitAsync(JobTarget.Colocated(FakeServer.ExistingTableName, keyTuple), job, null);

            StringAssert.Contains("priority = 999, maxRetries = 66", await colocatedRes.GetResultAsync());
        }

        [Test]
        [TestCase("1E3", -3)]
        [TestCase("1.12E5", 0)]
        [TestCase("1.123456789", 10)]
        [TestCase("1.123456789", 5)]
        public async Task TestBigDecimalPropagation(string number, int scale)
        {
            var res = await Client.Compute.SubmitAsync(await GetNodeAsync(1), DecimalJob, $"{number},{scale}");
            var resVal = await res.GetResultAsync();

            var expected = decimal.Parse(number, NumberStyles.Float);

            if (scale > 0)
            {
                expected = decimal.Round(expected, scale);
            }

            Assert.AreEqual(expected, resVal);
        }

        [Test]
        public async Task TestMapReduceNodeNameTask()
        {
            Instant beforeStart = SystemClock.Instance.GetCurrentInstant();

            ITaskExecution<string> taskExec = await Client.Compute.SubmitMapReduceAsync(NodeNameTask, "+arg");

            // Result.
            string nodeNameString = await taskExec.GetResultAsync();
            string[] nodeNames = nodeNameString.Split(',');

            var expectedNodeNames = (await Client.GetClusterNodesAsync())
                .Select(x => x.Name + "+arg")
                .ToList();

            CollectionAssert.AreEquivalent(expectedNodeNames, nodeNames);

            // Execution.
            Assert.AreNotEqual(Guid.Empty, taskExec.Id);
            Assert.AreEqual(expectedNodeNames.Count, taskExec.JobIds.Count);
            CollectionAssert.DoesNotContain(taskExec.JobIds, Guid.Empty);

            // Task state.
            TaskState? state = await taskExec.GetStateAsync();

            Assert.IsNotNull(state);
            Assert.AreEqual(TaskStatus.Completed, state.Status);
            Assert.AreEqual(taskExec.Id, state.Id);
            Assert.That(state.CreateTime, Is.GreaterThan(beforeStart));
            Assert.That(state.StartTime, Is.GreaterThan(state.CreateTime));
            Assert.That(state.FinishTime, Is.GreaterThan(state.StartTime));

            // Job states.
            IList<JobState?> jobStates = await taskExec.GetJobStatesAsync();

            Assert.AreEqual(expectedNodeNames.Count, jobStates.Count);

            foreach (var jobState in jobStates)
            {
                Assert.IsNotNull(jobState);
                Assert.AreEqual(JobStatus.Completed, jobState!.Status);
            }
        }

        [Test]
        public async Task TestExceptionInTask([Values(true, false)] bool splitOrReduce)
        {
            TaskDescriptor<object?, object?> task = splitOrReduce ? SplitExceptionTask : ReduceExceptionTask;

            var taskExec = await Client.Compute.SubmitMapReduceAsync(task, null);
            var ex = Assert.ThrowsAsync<IgniteException>(async () => await taskExec.GetResultAsync());

            var taskState = await taskExec.GetStateAsync();
            var jobStates = await taskExec.GetJobStatesAsync();

            // Result - exception.
            Assert.AreEqual("Custom job error", ex.Message);
            StringAssert.Contains("ItThinClientComputeTest$CustomException", ex.InnerException!.Message);
            Assert.AreEqual(ErrorGroups.Table.ColumnAlreadyExists, ex.Code);

            // Failed task state.
            Assert.IsNotNull(taskState);
            Assert.IsNotNull(taskState.FinishTime);
            Assert.AreEqual(TaskStatus.Failed, taskState.Status);

            if (splitOrReduce)
            {
                // Exception in split: no jobs were created.
                Assert.AreEqual(0, taskExec.JobIds.Count);
                Assert.AreEqual(0, jobStates.Count);
            }
            else
            {
                // Exception in reduce: all jobs succeeded.
                Assert.That(taskExec.JobIds.Count, Is.GreaterThan(1));

                foreach (var jobState in jobStates)
                {
                    Assert.IsNotNull(jobState);
                    Assert.AreEqual(JobStatus.Completed, jobState!.Status);
                }
            }
        }

        [Test]
        public async Task TestCancelCompletedTask()
        {
            var taskExec = await Client.Compute.SubmitMapReduceAsync(NodeNameTask, "arg");

            await taskExec.GetResultAsync();
            var cancelRes = await taskExec.CancelAsync();
            var state = await taskExec.GetStateAsync();

            Assert.IsFalse(cancelRes);
            Assert.AreEqual(TaskStatus.Completed, state!.Status);
        }

        [Test]
        public async Task TestCancelExecutingTask()
        {
            var taskExec = await Client.Compute.SubmitMapReduceAsync(SleepTask, 3000);

            var state1 = await taskExec.GetStateAsync();
            Assert.AreEqual(TaskStatus.Executing, state1!.Status);

            var cancelRes = await taskExec.CancelAsync();
            var state2 = await taskExec.GetStateAsync();

            Assert.IsTrue(cancelRes);
            Assert.AreEqual(TaskStatus.Failed, state2!.Status);

            var ex = Assert.ThrowsAsync<ComputeException>(async () => await taskExec.GetResultAsync());
            StringAssert.Contains("CancellationException", ex.Message);
        }

        [Test]
        public async Task TestChangeTaskPriority()
        {
            var taskExec = await Client.Compute.SubmitMapReduceAsync(SleepTask, 3000);

            var state = await taskExec.GetStateAsync();
            Assert.AreEqual(TaskStatus.Executing, state!.Status);

            var changePriorityRes = await taskExec.ChangePriorityAsync(1000);
            Assert.IsFalse(changePriorityRes);
        }

        [Test]
        public async Task TestNestedObjectsWithJsonMarshaller()
        {
            var job = new JobDescriptor<MyArg, MyResult>(PlatformTestNodeRunner + "$JsonMarshallerJob")
            {
                ArgMarshaller = new JsonMarshaller<MyArg>(),
                ResultMarshaller = new JsonMarshaller<MyResult>()
            };

            var arg = new MyArg(1, "foo", new Nested(Guid.NewGuid(), 1.234m));

            var exec = await Client.Compute.SubmitAsync(await GetNodeAsync(1), job, arg);
            MyResult res = await exec.GetResultAsync();

            Assert.AreEqual("foo_1", res.Data);
            Assert.AreEqual(arg.Nested, res.Nested);
        }

        private static async Task AssertJobStatus<T>(IJobExecution<T> jobExecution, JobStatus status, Instant beforeStart)
        {
            JobState? state = await jobExecution.GetStateAsync();

            Assert.IsNotNull(state);
            Assert.AreEqual(jobExecution.Id, state!.Id);
            Assert.AreEqual(status, state.Status);
            Assert.Greater(state.CreateTime, beforeStart);
            Assert.Greater(state.StartTime, state.CreateTime);

            if (status is JobStatus.Canceled or JobStatus.Completed or JobStatus.Failed)
            {
                Assert.Greater(state.FinishTime, state.StartTime);
            }
            else
            {
                Assert.IsNull(state.FinishTime);
            }
        }

        private async Task<IJobTarget<IClusterNode>> GetNodeAsync(int index) =>
            JobTarget.Node(
                (await Client.GetClusterNodesAsync()).OrderBy(n => n.Name).Skip(index).First());

        private record Nested(Guid Id, decimal Price);

        private record MyArg(int Id, string Name, Nested Nested);

        private record MyResult(string Data, Nested Nested);
    }
}
