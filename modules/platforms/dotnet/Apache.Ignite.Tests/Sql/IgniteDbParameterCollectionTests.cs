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

namespace Apache.Ignite.Tests.Sql;

using Ignite.Sql;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="IgniteDbParameterCollection"/>.
/// </summary>
public class IgniteDbParameterCollectionTests
{
    [Test]
    public void TestAddAndRetrieve()
    {
        var collection = new IgniteDbParameterCollection();
        var param = new IgniteDbParameter { ParameterName = "p1" };

        collection.Add(param);

        Assert.AreEqual(1, collection.Count);
        Assert.AreSame(param, collection[0]);
        Assert.AreSame(param, collection["p1"]);
    }

    [Test]
    public void TestRemove()
    {
        var collection = new IgniteDbParameterCollection();
        var param = new IgniteDbParameter();

        collection.Add(param);
        collection.Remove(param);

        Assert.AreEqual(0, collection.Count);
    }

    [Test]
    public void TestContains()
    {
        var collection = new IgniteDbParameterCollection();
        var param = new IgniteDbParameter { ParameterName = "p1" };

        collection.Add(param);

        Assert.IsTrue(collection.Contains(param));
        Assert.IsTrue(collection.Contains("p1"));
    }

    [Test]
    public void TestClear()
    {
        var collection = new IgniteDbParameterCollection
        {
            new IgniteDbParameter(),
            new IgniteDbParameter()
        };

        collection.Clear();

        Assert.AreEqual(0, collection.Count);
    }

    [Test]
    public void TestIndexOf()
    {
        var collection = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };

        collection.Add(param1);
        collection.Add(param2);

        Assert.AreEqual(0, collection.IndexOf(param1));
        Assert.AreEqual(1, collection.IndexOf("p2"));
    }
}
