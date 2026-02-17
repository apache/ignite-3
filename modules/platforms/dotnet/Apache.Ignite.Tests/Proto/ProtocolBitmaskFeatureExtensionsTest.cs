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

namespace Apache.Ignite.Tests.Proto;

using System;
using Internal.Proto;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="ProtocolBitmaskFeatureExtensions"/>.
/// </summary>
public class ProtocolBitmaskFeatureExtensionsTest
{
    [Test]
    public void TestToBytesWithSingleFeature()
    {
        var bytes = ProtocolBitmaskFeature.UserAttributes.ToBytes();

        Assert.AreEqual(4, bytes.Length);
        Assert.AreEqual(1, bytes[0]);
        Assert.AreEqual(0, bytes[1]);
        Assert.AreEqual(0, bytes[2]);
        Assert.AreEqual(0, bytes[3]);
    }

    [Test]
    public void TestToBytesWithMultipleFeatures()
    {
        var features = ProtocolBitmaskFeature.UserAttributes |
                       ProtocolBitmaskFeature.TableReqsUseQualifiedName |
                       ProtocolBitmaskFeature.TxDirectMapping;

        var bytes = features.ToBytes();

        Assert.AreEqual(4, bytes.Length);
        Assert.AreEqual(7, bytes[0]); // 1 + 2 + 4 = 7
        Assert.AreEqual(0, bytes[1]);
        Assert.AreEqual(0, bytes[2]);
        Assert.AreEqual(0, bytes[3]);
    }

    [Test]
    public void TestToBytesWithHighBitFeatures()
    {
        var bytes = ProtocolBitmaskFeature.SqlPartitionAwarenessTableName.ToBytes();

        Assert.AreEqual(4, bytes.Length);
        Assert.AreEqual(0, bytes[0]);
        Assert.AreEqual(0, bytes[1]);
        Assert.AreEqual(1, bytes[2]); // Bit 16 = 65536 = 0x010000 (little-endian)
        Assert.AreEqual(0, bytes[3]);
    }

    [Test]
    public void TestToBytesWithAllFeatures()
    {
        var features = ProtocolBitmaskFeature.UserAttributes |
                       ProtocolBitmaskFeature.TableReqsUseQualifiedName |
                       ProtocolBitmaskFeature.TxDirectMapping |
                       ProtocolBitmaskFeature.PlatformComputeJob |
                       ProtocolBitmaskFeature.PlatformComputeExecutor |
                       ProtocolBitmaskFeature.StreamerReceiverExecutionOptions |
                       ProtocolBitmaskFeature.SqlPartitionAwareness |
                       ProtocolBitmaskFeature.SqlPartitionAwarenessTableName;

        var bytes = features.ToBytes();

        Assert.AreEqual(4, bytes.Length);

        Assert.AreEqual(63, bytes[0]);
        Assert.AreEqual(2, bytes[1]);
        Assert.AreEqual(1, bytes[2]);
        Assert.AreEqual(0, bytes[3]);
    }

    [Test]
    public void TestFromBytesWithFourBytes()
    {
        byte[] bytes = [7, 0, 0, 0];
        var features = ProtocolBitmaskFeatureExtensions.FromBytes(bytes);

        var expected = ProtocolBitmaskFeature.UserAttributes |
                       ProtocolBitmaskFeature.TableReqsUseQualifiedName |
                       ProtocolBitmaskFeature.TxDirectMapping;

        Assert.AreEqual(expected, features);
    }

    [Test]
    public void TestFromBytesWithLessThanFourBytes()
    {
        byte[] bytes = [7];
        var features = ProtocolBitmaskFeatureExtensions.FromBytes(bytes);

        var expected = ProtocolBitmaskFeature.UserAttributes |
                       ProtocolBitmaskFeature.TableReqsUseQualifiedName |
                       ProtocolBitmaskFeature.TxDirectMapping;

        Assert.AreEqual(expected, features);
    }

    [Test]
    public void TestFromBytesWithTwoBytes()
    {
        byte[] bytes = [0, 2];
        var features = ProtocolBitmaskFeatureExtensions.FromBytes(bytes);

        Assert.AreEqual(ProtocolBitmaskFeature.SqlPartitionAwareness, features);
    }

    [Test]
    public void TestFromBytesWithThreeBytes()
    {
        byte[] bytes = [0, 0, 1];
        var features = ProtocolBitmaskFeatureExtensions.FromBytes(bytes);

        Assert.AreEqual(ProtocolBitmaskFeature.SqlPartitionAwarenessTableName, features);
    }

    [Test]
    public void TestFromBytesWithEmptyArray()
    {
        byte[] bytes = [];
        var features = ProtocolBitmaskFeatureExtensions.FromBytes(bytes);

        Assert.AreEqual((ProtocolBitmaskFeature)0, features);
    }

    [Test]
    public void TestFromBytesWithMoreThanFourBytesThrows()
    {
        byte[] bytes = [1, 2, 3, 4, 5];

        var ex = Assert.Throws<InvalidOperationException>(() => ProtocolBitmaskFeatureExtensions.FromBytes(bytes));
        StringAssert.Contains("Invalid bitmask feature length: 5", ex!.Message);
    }

    [Test]
    public void TestRoundTripAllFeatures()
    {
        var original = ProtocolBitmaskFeature.UserAttributes |
                       ProtocolBitmaskFeature.TableReqsUseQualifiedName |
                       ProtocolBitmaskFeature.TxDirectMapping |
                       ProtocolBitmaskFeature.PlatformComputeJob |
                       ProtocolBitmaskFeature.PlatformComputeExecutor |
                       ProtocolBitmaskFeature.StreamerReceiverExecutionOptions |
                       ProtocolBitmaskFeature.SqlPartitionAwareness |
                       ProtocolBitmaskFeature.SqlPartitionAwarenessTableName;

        var bytes = original.ToBytes();
        var restored = ProtocolBitmaskFeatureExtensions.FromBytes(bytes);

        Assert.AreEqual(original, restored);
    }
}
