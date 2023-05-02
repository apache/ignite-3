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
using Ignite.Sql;
using Internal.Sql;
using NUnit.Framework;

/// <summary>
/// Tests for <see cref="ColumnTypeExtensions"/>.
/// </summary>
public class ColumnTypeExtensionsTests
{
    private static readonly ColumnType[] SqlColumnTypes = Enum.GetValues<ColumnType>();

    [TestCaseSource(nameof(SqlColumnTypes))]
    public void TestToClrType(ColumnType columnType) =>
        Assert.IsNotNull(columnType.ToClrType(), columnType.ToString());

    [TestCaseSource(nameof(SqlColumnTypes))]
    public void TestToSqlColumnType(ColumnType columnType) =>
        Assert.AreEqual(columnType, columnType.ToClrType().ToColumnType());

    [TestCaseSource(nameof(SqlColumnTypes))]
    public void TestSqlColumnTypeToSqlTypeName(ColumnType columnType)
    {
        if (columnType is ColumnType.Duration or ColumnType.Period)
        {
            return;
        }

        Assert.IsNotNull(columnType.ToSqlTypeName(), columnType.ToString());
    }

    [TestCaseSource(nameof(SqlColumnTypes))]
    public void TestClrTypeToSqlTypeName(ColumnType columnType)
    {
        if (columnType is ColumnType.Duration or ColumnType.Period)
        {
            return;
        }

        Assert.IsNotNull(columnType.ToClrType().ToSqlTypeName(), columnType.ToString());
    }
}
