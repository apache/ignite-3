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
using System.Linq;
using System.Threading.Tasks;
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
        var replicasNodes = replicas.Values.Distinct().OrderBy(x => x.Address.Port).ToList();
        var replicasPartitions = replicas.Keys.Select(x => ((HashPartition)x).PartitionId).OrderBy(x => x).ToList();

        var expectedNodes = (await Client.GetClusterNodesAsync()).OrderBy(x => x.Address.Port).ToList();

        CollectionAssert.AreEqual(expectedNodes, replicasNodes, "Primary replicas should be distributed among all nodes");

        CollectionAssert.AreEqual(
            Enumerable.Range(0, TablePartitionCount),
            replicasPartitions,
            "Primary replicas map should have all partitions");
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
    public void TestGetPrimaryReplicaUnknownPartitionIdThrows()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.PartitionManager.GetPrimaryReplicaAsync(new HashPartition(-1)));

        Assert.AreEqual("Primary replica not found for partition: HashPartition { PartitionId = -1 }", ex.Message);
    }

    [Test]
    public void TestGetPrimaryReplicaUnknownPartitionClassThrows()
    {
        var ex = Assert.ThrowsAsync<ArgumentException>(
            async () => await Table.PartitionManager.GetPrimaryReplicaAsync(new MyPartition()));

        Assert.AreEqual($"Unsupported partition type: {typeof(MyPartition)}", ex.Message);
    }

    [Test]
    [TestCase(0, 5)]
    [TestCase(1, 5)]
    [TestCase(2, 2)]
    [TestCase(3, 9)]
    [TestCase(4, 4)]
    [TestCase(5, 3)]
    [TestCase(7, 1)]
    [TestCase(8, 8)]
    [TestCase(9, 2)]
    public async Task TesGetPartitionForTupleKey(int id, int expectedPartition)
    {
        var partition = await Table.PartitionManager.GetPartitionAsync(GetTuple(id));

        Assert.AreEqual(expectedPartition, ((HashPartition)partition).PartitionId);
    }

    [Test]
    public async Task TesGetPartitionForPocoKey()
    {
        await Task.Delay(1);
    }

    private class MyPartition : IPartition
    {
        // No-op.
    }
}
