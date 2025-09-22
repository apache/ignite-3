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
using System.Data;
using Ignite.Sql;
using NUnit.Framework;

public class IgniteDbParameterTests
{
    [Test]
    public void TestDefaults()
    {
        var param = new IgniteDbParameter();
        Assert.AreEqual(DbType.String, param.DbType);
        Assert.AreEqual(ParameterDirection.Input, param.Direction);
        Assert.IsFalse(param.IsNullable);
        Assert.AreEqual(string.Empty, param.ParameterName);
        Assert.AreEqual(string.Empty, param.SourceColumn);
        Assert.IsNull(param.Value);
        Assert.IsFalse(param.SourceColumnNullMapping);
        Assert.AreEqual(0, param.Size);
    }

    [Test]
    public void TestDirectionOnlyInputAllowed()
    {
        var param = new IgniteDbParameter();
        Assert.AreEqual(ParameterDirection.Input, param.Direction);
        Assert.Throws<ArgumentException>(() => param.Direction = ParameterDirection.Output);
    }

    [Test]
    public void TestResetDbTypeSetsString()
    {
        var param = new IgniteDbParameter { DbType = DbType.Boolean };
        param.ResetDbType();
        Assert.AreEqual(DbType.String, param.DbType);
    }

    [Test]
    public void TestToString()
    {
        var param = new IgniteDbParameter { Value = 12.3 };

        Assert.AreEqual("IgniteDbParameter { Value = 12.3 }", param.ToString());
    }
}
