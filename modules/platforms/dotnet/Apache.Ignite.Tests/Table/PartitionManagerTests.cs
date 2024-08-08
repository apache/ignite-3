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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Compute;
using Ignite.Compute;
using Ignite.Table;
using Internal.Table;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IPartitionManager"/>.
/// </summary>
public class PartitionManagerTests : IgniteTestsBase
{
    [Test]
    public async Task TestGetPrimaryReplicas()
    {
        var replicas = await Table.PartitionManager.GetPrimaryReplicasAsync();
        var replicasNodes = replicas.Values.Distinct().OrderBy(x => ((IPEndPoint)x.Address).Port).ToList();
        var replicasPartitions = replicas.Keys.Select(x => ((HashPartition)x).PartitionId).OrderBy(x => x).ToList();

        var expectedNodes = (await Client.GetClusterNodesAsync())
            .OrderBy(x => ((IPEndPoint)x.Address).Port)
            .ToList();

        CollectionAssert.AreEqual(expectedNodes, replicasNodes, "Primary replicas should be distributed among all nodes");

        CollectionAssert.AreEqual(
            Enumerable.Range(0, TablePartitionCount),
            replicasPartitions,
            "Primary replicas map should have all partitions");
    }

    [Test]
    public async Task TestGetPrimaryReplicasReturnsCachedPartitionInstances()
    {
        var partitions1 = await GetPartitions();
        var partitions2 = await GetPartitions();

        for (int i = 0; i < partitions1.Count; i++)
        {
            Assert.AreSame(partitions1[i], partitions2[i]);
        }

        async Task<List<HashPartition>> GetPartitions()
        {
            var replicas = await Table.PartitionManager.GetPrimaryReplicasAsync();
            return replicas.Keys.Cast<HashPartition>().OrderBy(x => x.PartitionId).ToList();
        }
    }

    [Test]
    public async Task TestGetPrimaryReplica()
    {
        var nodes = await Client.GetClusterNodesAsync();

        for (int partId = 0; partId < TablePartitionCount; partId++)
        {
            var partition = new HashPartition(partId);
            var replica = await Table.PartitionManager.GetPrimaryReplicaAsync(partition);

            CollectionAssert.Contains(nodes, replica);
        }
    }

    [Test]
    public void TestGetPrimaryReplicaNegativePartitionIdThrows()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.PartitionManager.GetPrimaryReplicaAsync(new HashPartition(-1)));

        Assert.AreEqual("Partition id can't be negative: HashPartition { PartitionId = -1 }", ex.Message);
    }

    [Test]
    public void TestGetPrimaryReplicaPartitionIdOutOfRangeThrows()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.PartitionManager.GetPrimaryReplicaAsync(new HashPartition(10)));

        Assert.AreEqual("Partition id can't be greater than 9: HashPartition { PartitionId = 10 }", ex.Message);
    }

    [Test]
    public void TestGetPrimaryReplicaUnknownPartitionClassThrows()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.PartitionManager.GetPrimaryReplicaAsync(new MyPartition()));

        Assert.AreEqual($"Unsupported partition type: {typeof(MyPartition)}", ex.Message);
    }

    [Test]
    public async Task TestGetPartitionForKey([Values(true, false)] bool poco)
    {
        var jobTarget = JobTarget.AnyNode(await Client.GetClusterNodesAsync());

        for (int id = 0; id < 30; id++)
        {
            var partition = poco
                ? await Table.PartitionManager.GetPartitionAsync(GetPoco(id))
                : await Table.PartitionManager.GetPartitionAsync(GetTuple(id));

            var partitionJobExec = await Client.Compute.SubmitAsync(jobTarget, ComputeTests.PartitionJob, id);
            var expectedPartition = await partitionJobExec.GetResultAsync();

            Assert.AreEqual(expectedPartition, ((HashPartition)partition).PartitionId);
        }
    }

    [Test]
    public async Task TestGetPartitionReturnsCachedInstance()
    {
        var partition1 = await Table.PartitionManager.GetPartitionAsync(GetTuple(1));
        var partition2 = await Table.PartitionManager.GetPartitionAsync(GetTuple(1));

        Assert.AreSame(partition1, partition2);
    }

    [Test]
    public async Task TestPrimaryReplicaCacheInvalidation()
    {
        using var server = new FakeServer
        {
            PartitionAssignmentTimestamp = 123,
            PartitionAssignment = new[] { "n1", "n2" }
        };

        using var client = await server.ConnectClientAsync();
        var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
        var partition = new HashPartition(0);

        var replica1 = await table!.PartitionManager.GetPrimaryReplicaAsync(partition);
        Assert.AreEqual("n1", replica1.Name);

        server.PartitionAssignmentTimestamp = 124;
        server.PartitionAssignment = new[] { "n2", "n1" };

        await client.Tables.GetTablesAsync(); // Trigger cache invalidation with any response.

        var replica2 = await table.PartitionManager.GetPrimaryReplicaAsync(partition);
        Assert.AreEqual("n2", replica2.Name);
    }

    [Test]
    public void TestPartitionEquality()
    {
        IPartition part1 = new HashPartition(1);
        IPartition part1Copy = new HashPartition(1);
        IPartition part2 = new HashPartition(2);
        IPartition customPart = new MyPartition();

        Assert.IsTrue(part1.Equals(part1Copy));
        Assert.IsFalse(part1.Equals(part2));
        Assert.IsFalse(part1.Equals(customPart));
    }

    private class MyPartition : IPartition
    {
        public bool Equals(IPartition? other) => false;
    }
}
