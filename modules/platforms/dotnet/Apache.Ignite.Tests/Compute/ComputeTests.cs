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
    using System.Linq;
    using System.Net;
    using System.Numerics;
    using System.Threading.Tasks;
    using Ignite.Compute;
    using Ignite.Table;
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

        private const string ItThinClientComputeTest = "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest";

        private const string ConcatJob = ItThinClientComputeTest + "$ConcatJob";

        private const string ErrorJob = ItThinClientComputeTest + "$IgniteExceptionJob";

        private const string EchoJob = ItThinClientComputeTest + "$EchoJob";

        private const string SleepJob = ItThinClientComputeTest + "$SleepJob";

        private const string PlatformTestNodeRunner = "org.apache.ignite.internal.runner.app.PlatformTestNodeRunner";

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
            var res1 = await Client.Compute.ExecuteAsync<string>(await GetNodeAsync(0), Units, NodeNameJob, "-", 11);
            var res2 = await Client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), Units, NodeNameJob, ":", 22);

            Assert.AreEqual(PlatformTestNodeRunner + "-_11", res1);
            Assert.AreEqual(PlatformTestNodeRunner + "_2:_22", res2);
        }

        [Test]
        public async Task TestExecuteOnRandomNode()
        {
            var res = await Client.Compute.ExecuteAsync<string>(await Client.GetClusterNodesAsync(), Units, NodeNameJob);

            var expectedNodeNames = Enumerable.Range(1, 4)
                .Select(x => x == 1 ? PlatformTestNodeRunner : PlatformTestNodeRunner + "_" + x)
                .ToList();

            CollectionAssert.Contains(expectedNodeNames, res);
        }

        [Test]
        public void TestExecuteResultTypeMismatchThrowsInvalidCastException()
        {
            Assert.ThrowsAsync<InvalidCastException>(async () =>
                await Client.Compute.ExecuteAsync<Guid>(await Client.GetClusterNodesAsync(), Units, NodeNameJob));
        }

        [Test]
        public async Task TestBroadcastOneNode()
        {
            var nodes = await GetNodeAsync(0);

            IDictionary<IClusterNode, Task<string>> taskMap = Client.Compute.BroadcastAsync<string>(nodes, Units, NodeNameJob, "123");
            var res = await taskMap[nodes[0]];

            Assert.AreEqual(1, taskMap.Count);
            Assert.AreSame(nodes[0], taskMap.Keys.Single());

            Assert.AreEqual(PlatformTestNodeRunner + "123", res);
        }

        [Test]
        public async Task TestBroadcastAllNodes()
        {
            var nodes = await Client.GetClusterNodesAsync();

            IDictionary<IClusterNode, Task<string>> taskMap = Client.Compute.BroadcastAsync<string>(nodes, Units, NodeNameJob, "123");
            var res1 = await taskMap[nodes[0]];
            var res2 = await taskMap[nodes[1]];
            var res3 = await taskMap[nodes[2]];
            var res4 = await taskMap[nodes[3]];

            Assert.AreEqual(4, taskMap.Count);

            Assert.AreEqual(nodes[0].Name + "123", res1);
            Assert.AreEqual(nodes[1].Name + "123", res2);
            Assert.AreEqual(nodes[2].Name + "123", res3);
            Assert.AreEqual(nodes[3].Name + "123", res4);
        }

        [Test]
        public async Task TestExecuteWithArgs()
        {
            var res = await Client.Compute.ExecuteAsync<string>(await Client.GetClusterNodesAsync(), Units, ConcatJob, 1.1, Guid.Empty, "3", null);

            Assert.AreEqual("1.1_00000000-0000-0000-0000-000000000000_3_null", res);
        }

        [Test]
        public async Task TestExecuteWithNullArgs()
        {
            var res = await Client.Compute.ExecuteAsync<string>(await Client.GetClusterNodesAsync(), Units, ConcatJob, args: null);

            Assert.IsNull(res);
        }

        [Test]
        public void TestJobErrorPropagatesToClientWithClassAndMessage()
        {
            var ex = Assert.ThrowsAsync<IgniteException>(async () =>
                await Client.Compute.ExecuteAsync<string>(await Client.GetClusterNodesAsync(), Units, ErrorJob, "unused"));

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
        public void TestUnknownNodeThrows()
        {
            var unknownNode = new ClusterNode("x", "y", new IPEndPoint(IPAddress.Loopback, 0));

            var ex = Assert.ThrowsAsync<IgniteException>(async () =>
                await Client.Compute.ExecuteAsync<string>(new[] { unknownNode }, Units, EchoJob, "unused"));

            StringAssert.Contains("Specified node is not present in the cluster: y", ex!.Message);
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
                var res = await Client.Compute.ExecuteAsync<object>(nodes, Units, EchoJob, val, str);

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
            var resNodeName = await client.Compute.ExecuteColocatedAsync<string>(TableName, keyTuple, Units, NodeNameJob);
            var requestTargetNodeName = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var keyPoco = new Poco { Key = key };
            var resNodeName2 = await client.Compute.ExecuteColocatedAsync<string, Poco>(TableName, keyPoco, Units.Reverse(), NodeNameJob);
            var requestTargetNodeName2 = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var keyPocoStruct = new PocoStruct(key, null);
            var resNodeName3 = await client.Compute.ExecuteColocatedAsync<string, PocoStruct>(TableName, keyPocoStruct, Units, NodeNameJob);
            var requestTargetNodeName3 = GetRequestTargetNodeName(proxies, ClientOp.ComputeExecuteColocated);

            var nodeName = nodeIdx == 1 ? string.Empty : "_" + nodeIdx;
            var expectedNodeName = PlatformTestNodeRunner + nodeName;

            Assert.AreEqual(expectedNodeName, resNodeName);
            Assert.AreEqual(expectedNodeName, resNodeName2);
            Assert.AreEqual(expectedNodeName, resNodeName3);

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
                await Client.Compute.ExecuteColocatedAsync<string>("unknownTable", new IgniteTuple(), Units, EchoJob));

            Assert.AreEqual("Table 'unknownTable' does not exist.", ex!.Message);
        }

        [Test]
        public void TestExecuteColocatedThrowsWhenKeyColumnIsMissing()
        {
            var ex = Assert.ThrowsAsync<ArgumentException>(async () =>
                await Client.Compute.ExecuteColocatedAsync<string>(TableName, new IgniteTuple { ["VAL"] = "1" }, Units, EchoJob));

            Assert.AreEqual(
                "Can't map 'IgniteTuple { VAL = 1 }' to columns 'Int64 KEY, String VAL'. Matching fields not found.",
                ex!.Message);
        }

        [Test]
        public async Task TestExecuteColocatedUpdatesTableCacheOnTableDrop([Values(false, true)] bool forceLoadAssignment)
        {
            // Create table and use it in ExecuteColocated.
            var nodes = await GetNodeAsync(0);
            var tableName = await Client.Compute.ExecuteAsync<string>(nodes, Units, CreateTableJob, "drop_me");

            try
            {
                var keyTuple = new IgniteTuple { [KeyCol] = 1L };
                var resNodeName = await Client.Compute.ExecuteColocatedAsync<string>(tableName, keyTuple, Units, NodeNameJob);

                // Drop table and create a new one with a different ID, then execute a computation again.
                // This should update the cached table and complete the computation successfully.
                await Client.Compute.ExecuteAsync<string>(nodes, Units, DropTableJob, tableName);
                await Client.Compute.ExecuteAsync<string>(nodes, Units, CreateTableJob, tableName);

                if (forceLoadAssignment)
                {
                    var table = Client.Compute.GetFieldValue<IDictionary>("_tableCache")[tableName]!;
                    table.SetFieldValue("_partitionAssignment", null);
                }

                var resNodeName2 = await Client.Compute.ExecuteColocatedAsync<string>(tableName, keyTuple, Units, NodeNameJob);

                Assert.AreEqual(resNodeName, resNodeName2);
            }
            finally
            {
                await Client.Compute.ExecuteAsync<string>(nodes, Units, DropTableJob, tableName);
            }
        }

        [Test]
        public void TestExceptionInJobWithSendServerExceptionStackTraceToClientPropagatesToClientWithStackTrace()
        {
            var ex = Assert.ThrowsAsync<IgniteException>(async () =>
                await Client.Compute.ExecuteAsync<object>(await GetNodeAsync(1), Units, ExceptionJob, "foo-bar"));

            Assert.AreEqual("Test exception: foo-bar", ex!.Message);
            Assert.IsNotNull(ex.InnerException);

            var str = ex.ToString();

            // TODO IGNITE-20858: Fix once user errors are handled properly
            StringAssert.Contains("Apache.Ignite.IgniteException: Test exception: foo-bar", str);
            StringAssert.Contains(
                "at org.apache.ignite.internal.runner.app.PlatformTestNodeRunner$ExceptionJob.execute(PlatformTestNodeRunner.java:",
                str);
        }

        [Test]
        public void TestCheckedExceptionInJobPropagatesToClient()
        {
            var ex = Assert.ThrowsAsync<IgniteException>(async () =>
                await Client.Compute.ExecuteAsync<object>(await GetNodeAsync(1), Units, CheckedExceptionJob, "foo-bar"));

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

            var res = await client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), units, FakeServer.GetDetailsJob);
            StringAssert.Contains("Units = unit-latest|latest, unit1|1.0.0", res);

            // Lazy enumerable.
            var res2 = await client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), units.Reverse(), FakeServer.GetDetailsJob);
            StringAssert.Contains("Units = unit1|1.0.0, unit-latest|latest", res2);

            // Colocated.
            var keyTuple = new IgniteTuple { ["ID"] = 1 };
            var res3 = await client.Compute.ExecuteColocatedAsync<string>(
                FakeServer.ExistingTableName, keyTuple, units, FakeServer.GetDetailsJob);

            StringAssert.Contains("Units = unit-latest|latest, unit1|1.0.0", res3);
        }

        [Test]
        public void TestExecuteOnUnknownUnitWithLatestVersionThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new("unit-latest") };

            var ex = Assert.ThrowsAsync<IgniteException>(
                async () => await Client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            StringAssert.Contains("Deployment unit unit-latest:latest doesn't exist", ex!.Message);
        }

        [Test]
        public void TestExecuteColocatedOnUnknownUnitWithLatestVersionThrows()
        {
            var keyTuple = new IgniteTuple { [KeyCol] = 1L };
            var deploymentUnits = new DeploymentUnit[] { new("unit-latest") };

            var ex = Assert.ThrowsAsync<IgniteException>(
                async () => await Client.Compute.ExecuteColocatedAsync<string>(TableName, keyTuple, deploymentUnits, NodeNameJob));

            StringAssert.Contains("Deployment unit unit-latest:latest doesn't exist", ex!.Message);
        }

        [Test]
        public void TestNullUnitNameThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new(null!) };

            var ex = Assert.ThrowsAsync<ArgumentNullException>(
                async () => await Client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("Value cannot be null. (Parameter 'unit.Name')", ex!.Message);
        }

        [Test]
        public void TestEmptyUnitNameThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new(string.Empty) };

            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("The value cannot be an empty string. (Parameter 'unit.Name')", ex!.Message);
        }

        [Test]
        public void TestNullUnitVersionThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new("u", null!) };

            var ex = Assert.ThrowsAsync<ArgumentNullException>(
                async () => await Client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("Value cannot be null. (Parameter 'unit.Version')", ex!.Message);
        }

        [Test]
        public void TestEmptyUnitVersionThrows()
        {
            var deploymentUnits = new DeploymentUnit[] { new("u", string.Empty) };

            var ex = Assert.ThrowsAsync<ArgumentException>(
                async () => await Client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), deploymentUnits, NodeNameJob));

            Assert.AreEqual("The value cannot be an empty string. (Parameter 'unit.Version')", ex!.Message);
        }

        [Test]
        public async Task TestDelayedJobExecutionThrowsWhenConnectionFails()
        {
            // Compute jobs are completed with a notification message.
            // In this test, the job execution starts, but we drop the client connection, so notification can't be received.
            using var client = await IgniteClient.StartAsync(GetConfig());

            const int sleepMs = 3000;
            var jobTask = client.Compute.ExecuteAsync<string>(await GetNodeAsync(1), Units, SleepJob, sleepMs);

            // Wait a bit and close the connection.
            await Task.Delay(10);
            client.Dispose();

            var ex = Assert.ThrowsAsync<IgniteClientConnectionException>(async () => await jobTask);
            Assert.AreEqual("Connection closed.", ex!.Message);
        }

        private async Task<List<IClusterNode>> GetNodeAsync(int index) =>
            (await Client.GetClusterNodesAsync()).OrderBy(n => n.Name).Skip(index).Take(1).ToList();
    }
}
