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
        var col = new IgniteDbParameterCollection();
        var param = new IgniteDbParameter { ParameterName = "p1" };

        col.Add(param);

        Assert.AreEqual(1, col.Count);
        Assert.AreSame(param, col[0]);
        Assert.AreSame(param, col["p1"]);
    }

    [Test]
    public void TestRemove()
    {
        var col = new IgniteDbParameterCollection();
        var param = new IgniteDbParameter();

        col.Add((object)param);
        col.Remove((object)param);

        Assert.AreEqual(0, col.Count);
    }

    [Test]
    public void TestContains()
    {
        var col = new IgniteDbParameterCollection();
        var param = new IgniteDbParameter { ParameterName = "p1" };

        col.Add(param);

        Assert.IsTrue(col.Contains(param));
        Assert.IsTrue(col.Contains((object)param));
        Assert.IsTrue(col.Contains("p1"));
    }

    [Test]
    public void TestClear()
    {
        var col = new IgniteDbParameterCollection
        {
            new IgniteDbParameter(),
            new IgniteDbParameter()
        };

        col.Clear();

        Assert.AreEqual(0, col.Count);
    }

    [Test]
    public void TestIndexOf()
    {
        var col = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };

        col.Add(param1);
        col.Add(param2);

        Assert.AreEqual(0, col.IndexOf(param1));
        Assert.AreEqual(0, col.IndexOf((object)param1));
        Assert.AreEqual(1, col.IndexOf("p2"));
    }

    [Test]
    public void TestAddRange()
    {
        var col = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };

        col.AddRange(new[] { param1, param2 });

        Assert.AreEqual(2, col.Count);
        Assert.AreSame(param1, col[0]);
        Assert.AreSame(param2, col[1]);
    }

    [Test]
    public void TestInsertAndRemoveAt()
    {
        var col = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };

        col.Add(param1);
        col.Insert(0, param2);

        Assert.AreSame(param2, col[0]);
        Assert.AreSame(param1, col[1]);

        col.RemoveAt(0);
        Assert.AreSame(param1, col[0]);

        col.RemoveAt("p1");
        Assert.AreEqual(0, col.Count);
    }

    [Test]
    public void TestSetAndGetParameter()
    {
        var col = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };
        col.Add(param1);

        // Set by index
        col[0] = param2;
        Assert.AreSame(param2, col[0]);

        // Set by name
        col.Add(param1);
        var dbParam = new IgniteDbParameter { ParameterName = "p3" };
        int idx = col.IndexOf("p2");
        col[idx] = dbParam;
        Assert.AreSame(dbParam, col[0]);

        // Get by index and name
        Assert.AreSame(dbParam, col[0]);
        Assert.AreSame(dbParam, col[col.IndexOf("p3")]);
    }

    [Test]
    public void TestCopyTo()
    {
        var col = new IgniteDbParameterCollection();
        var param1 = new IgniteDbParameter { ParameterName = "p1" };
        var param2 = new IgniteDbParameter { ParameterName = "p2" };

        col.Add(param1);
        col.Add(param2);

        var arr = new IgniteDbParameter[2];
        col.CopyTo(arr, 0);

        Assert.AreSame(param1, arr[0]);
        Assert.AreSame(param2, arr[1]);

        var arr2 = new IgniteDbParameter[2];
        ((ICollection)col).CopyTo(arr2, 0);

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

    [Test]
    public void TestToString()
    {
        var col = new IgniteDbParameterCollection
        {
            new IgniteDbParameter { Value = 1 },
            new IgniteDbParameter { Value = "x" }
        };

        Assert.AreEqual("IgniteDbParameterCollection { Count = 2 }", col.ToString());
    }
}
