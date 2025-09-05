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
        Assert.AreEqual(ColumnType.String, param.IgniteColumnType);
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
    public void TestDbTypeIgniteColumnTypeMapping()
    {
        AssertMapping(DbType.Binary, ColumnType.ByteArray);
        AssertMapping(DbType.Byte, ColumnType.Int8);
        AssertMapping(DbType.Boolean, ColumnType.Boolean);
        AssertMapping(DbType.Date, ColumnType.Date);
        AssertMapping(DbType.DateTime, ColumnType.Datetime);
        AssertMapping(DbType.Decimal, ColumnType.Decimal);
        AssertMapping(DbType.Double, ColumnType.Double);
        AssertMapping(DbType.Guid, ColumnType.Uuid);
        AssertMapping(DbType.Int16, ColumnType.Int16);
        AssertMapping(DbType.Int32, ColumnType.Int32);
        AssertMapping(DbType.Int64, ColumnType.Int64);
        AssertMapping(DbType.Single, ColumnType.Float);
        AssertMapping(DbType.String, ColumnType.String);
        AssertMapping(DbType.Time, ColumnType.Time);

        AssertUnsupported(DbType.DateTimeOffset);
        AssertUnsupported(DbType.AnsiString);
        AssertUnsupported(DbType.Currency);
        AssertUnsupported(DbType.Object);
        AssertUnsupported(DbType.UInt16);
        AssertUnsupported(DbType.UInt32);
        AssertUnsupported(DbType.UInt64);
        AssertUnsupported(DbType.SByte);
        AssertUnsupported(DbType.Xml);
        AssertUnsupported(DbType.VarNumeric);
        AssertUnsupported(DbType.AnsiStringFixedLength);
        AssertUnsupported(DbType.StringFixedLength);
        AssertUnsupported(DbType.DateTime2);

        return;

        static void AssertMapping(DbType dbType, ColumnType columnType)
        {
            var param = new IgniteDbParameter { DbType = dbType };
            Assert.AreEqual(columnType, param.IgniteColumnType);

            param.IgniteColumnType = columnType;
            Assert.AreEqual(dbType, param.DbType);
        }

        static void AssertUnsupported(DbType dbType)
        {
            var param = new IgniteDbParameter();
            Assert.Throws<NotSupportedException>(() => param.DbType = dbType);
        }
    }

    [Test]
    public void TestDirectionOnlyInputAllowed()
    {
        var param = new IgniteDbParameter();
        Assert.AreEqual(ParameterDirection.Input, param.Direction);
        Assert.Throws<ArgumentException>(() => param.Direction = ParameterDirection.Output);
    }

    [Test]
    public void TestParameterNameAndSourceColumnNullToEmpty()
    {
        var param = new IgniteDbParameter
        {
            ParameterName = null,
            SourceColumn = null
        };

        Assert.AreEqual(string.Empty, param.ParameterName);
        Assert.AreEqual(string.Empty, param.SourceColumn);
    }

    [Test]
    public void TestResetDbTypeSetsString()
    {
        var param = new IgniteDbParameter { IgniteColumnType = ColumnType.Int8 };
        param.ResetDbType();
        Assert.AreEqual(ColumnType.String, param.IgniteColumnType);
    }
}
