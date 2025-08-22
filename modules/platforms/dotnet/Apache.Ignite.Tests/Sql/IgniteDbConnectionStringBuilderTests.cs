// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.Tests.Sql;

using System;
using Ignite.Sql;
using NUnit.Framework;

public class IgniteDbConnectionStringBuilderTests
{
    [Test]
    public void TestCtorParsesConnectionString([Values("Data Source", "DataSource", "Endpoints")] string keyword)
    {
        var builder = new IgniteDbConnectionStringBuilder($"{keyword}=localhost:10800");

        Assert.AreEqual("localhost:10800", builder[keyword]);
    }

    [Test]
    public void TestToStringBuildsConnectionString()
    {
        var builder = new IgniteDbConnectionStringBuilder
        {
            Endpoints = ["localhost:10800", "localhost:10801"],
            SocketTimeout = TimeSpan.FromSeconds(2.5),
            OperationTimeout = TimeSpan.FromMinutes(1.2345)
        };

        Assert.AreEqual(
            "Endpoints=localhost:10800,localhost:10801;SocketTimeout=00:00:02.5000000;OperationTimeout=00:01:14.0700000",
            builder.ToString());
    }
}
