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

using System;
using System.Collections;
using System.Collections.Generic;
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

        collection.Add((object)param);
        collection.Remove((object)param);

        Assert.AreEqual(0, collection.Count);
    }

    [Test]
    public void TestContains()
    {
        var collection = new IgniteDbParameterCollection();
        var param = new IgniteDbParameter { ParameterName = "p1" };

        collection.Add(param);

        Assert.IsTrue(collection.Contains(param));
        Assert.IsTrue(collection.Contains((object)param));
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
        Assert.AreEqual(0, collection.IndexOf((object)param1));
        Assert.AreEqual(1, collection.IndexOf("p2"));
    }

    [Test]
    public void TestAddRange()
    {
        var collection = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };
        collection.AddRange(new[] { param1, param2 });
        Assert.AreEqual(2, collection.Count);
        Assert.AreSame(param1, collection[0]);
        Assert.AreSame(param2, collection[1]);
    }

    [Test]
    public void TestInsertAndRemoveAt()
    {
        var collection = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };
        collection.Add(param1);
        collection.Insert(0, param2);
        Assert.AreSame(param2, collection[0]);
        Assert.AreSame(param1, collection[1]);
        collection.RemoveAt(0);
        Assert.AreSame(param1, collection[0]);
        collection.RemoveAt("p1");
        Assert.AreEqual(0, collection.Count);
    }

    [Test]
    public void TestSetAndGetParameter()
    {
        var collection = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };
        collection.Add(param1);

        // Set by index
        collection[0] = param2;
        Assert.AreSame(param2, collection[0]);

        // Set by name (simulate by finding index and setting)
        collection.Add(param1);
        var dbParam = new IgniteDbParameter { ParameterName = "p3" };
        int idx = collection.IndexOf("p2");
        collection[idx] = dbParam;
        Assert.AreSame(dbParam, collection[0]);

        // Get by index and name
        Assert.AreSame(dbParam, collection[0]);
        Assert.AreSame(dbParam, collection[collection.IndexOf("p3")]);
    }

    [Test]
    public void TestCopyTo()
    {
        var collection = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };
        collection.Add(param1);
        collection.Add(param2);
        var arr = new IgniteDbParameter[2];
        collection.CopyTo(arr, 0);
        Assert.AreSame(param1, arr[0]);
        Assert.AreSame(param2, arr[1]);
        var arr2 = new IgniteDbParameter[2];
        ((ICollection)collection).CopyTo(arr2, 0);
        Assert.AreSame(param1, arr2[0]);
        Assert.AreSame(param2, arr2[1]);
    }

    [Test]
    public void TestEnumerator()
    {
        var collection = new IgniteDbParameterCollection();

        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };

        collection.Add(param1);
        collection.Add(param2);

        var list = new List<IgniteDbParameter>();
        foreach (var p in collection)
        {
            list.Add((IgniteDbParameter)p);
        }

        Assert.AreEqual(2, list.Count);
        Assert.Contains(param1, list);
        Assert.Contains(param2, list);
    }

    [Test]
    public void TestExceptions()
    {
        var collection = new IgniteDbParameterCollection();
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = collection[0]; });
        Assert.Throws<InvalidOperationException>(() => collection.RemoveAt("notfound"));
    }
}
