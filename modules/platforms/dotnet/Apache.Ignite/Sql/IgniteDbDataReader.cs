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

namespace Apache.Ignite.Sql;

using System;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data.Common;

/// <summary>
/// Reads a forward-only stream of rows from an Ignite result set.
/// </summary>
public sealed class IgniteDbDataReader : DbDataReader, IDbColumnSchemaGenerator
{
    /// <inheritdoc />
    public override bool GetBoolean(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override byte GetByte(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override DateTime GetDateTime(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override decimal GetDecimal(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override double GetDouble(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override float GetFloat(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override Guid GetGuid(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override short GetInt16(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override int GetInt32(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override long GetInt64(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override string GetName(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override string GetString(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override object GetValue(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override bool IsDBNull(int ordinal)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override int FieldCount { get; }

    /// <inheritdoc/>
    public override object this[int ordinal] => throw new NotImplementedException();

    /// <inheritdoc/>
    public override object this[string name] => throw new NotImplementedException();

    /// <inheritdoc/>
    public override int RecordsAffected { get; }

    /// <inheritdoc/>
    public override bool HasRows { get; }

    /// <inheritdoc/>
    public override bool IsClosed { get; }

    /// <inheritdoc/>
    public override bool NextResult()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override bool Read()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override int Depth { get; }

    /// <inheritdoc/>
    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public ReadOnlyCollection<DbColumn> GetColumnSchema()
    {
        throw new NotImplementedException();
    }
}
