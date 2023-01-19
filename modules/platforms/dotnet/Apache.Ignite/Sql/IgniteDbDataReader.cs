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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Internal.Buffers;
using Internal.Proto.BinaryTuple;
using Internal.Sql;

/// <summary>
/// Reads a forward-only stream of rows from an Ignite result set.
/// </summary>
[SuppressMessage("Design", "CA1010:Generic interface should also be implemented", Justification = "Generic IEnumerable is not applicable.")]
[SuppressMessage("Usage", "CA2215:Dispose methods should call base class dispose", Justification = "Base class dispose is empty.")]
public sealed class IgniteDbDataReader : DbDataReader, IDbColumnSchemaGenerator
{
    // TODO: Methods to read Ignite-specific types.
    private static readonly Task<bool> TrueTask = Task.FromResult(true);

    private readonly ResultSet<object> _resultSet;

    private readonly IAsyncEnumerator<PooledBuffer> _pageEnumerator;

    private int _pageRowCount = -1;

    private int _pageRowIndex = -1;

    private int _pageRowOffset = -1;

    private int _pageRowSize = -1;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbDataReader"/> class.
    /// </summary>
    /// <param name="resultSet">Result set.</param>
    internal IgniteDbDataReader(ResultSet<object> resultSet)
    {
        _resultSet = resultSet;

        // TODO: Should we support non-query result sets?
        _pageEnumerator = _resultSet.EnumeratePagesInternal().GetAsyncEnumerator();
    }

    /// <inheritdoc/>
    public override int FieldCount => Metadata.Columns.Count;

    /// <inheritdoc/>
    public override int RecordsAffected => checked((int)_resultSet.AffectedRows);

    /// <inheritdoc/>
    public override bool HasRows => _resultSet.HasRowSet;

    /// <inheritdoc/>
    public override bool IsClosed => false; // TODO: ??

    /// <summary>
    /// Gets a value indicating the depth of nesting for the current row. Always zero in Ignite.
    /// </summary>
    /// <returns>The level of nesting.</returns>
    public override int Depth => 0;

    /// <summary>
    /// Gets Ignite-specific result set metadata.
    /// </summary>
    public IResultSetMetadata Metadata => _resultSet.Metadata!;

    /// <inheritdoc/>
    public override object this[int ordinal] => null!; // TODO

    /// <inheritdoc/>
    public override object this[string name] => null!; // TODO

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => GetReader(ordinal, typeof(bool)).GetByteAsBool(ordinal);

    /// <inheritdoc/>
    public override byte GetByte(int ordinal) => unchecked((byte)GetReader(ordinal, typeof(sbyte)).GetByte(ordinal));

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
    public override short GetInt16(int ordinal) => GetReader(ordinal, typeof(short)).GetShort(ordinal);

    /// <inheritdoc/>
    public override int GetInt32(int ordinal) => GetReader(ordinal, typeof(int)).GetInt(ordinal);

    /// <inheritdoc/>
    public override long GetInt64(int ordinal) => GetReader(ordinal, typeof(long)).GetLong(ordinal);

    /// <inheritdoc/>
    public override string GetName(int ordinal) => Metadata.Columns[ordinal].Name;

    /// <inheritdoc/>
    public override int GetOrdinal(string name) => Metadata.IndexOf(name);

    /// <inheritdoc/>
    public override string GetString(int ordinal) => GetReader(ordinal, typeof(string)).GetString(ordinal);

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
    public override bool IsDBNull(int ordinal) => GetReader().IsNull(ordinal);

    /// <inheritdoc/>
    public override bool NextResult() => throw new NotSupportedException("Batched result sets are not supported.");

    /// <inheritdoc/>
    public override bool Read() => ReadNextRowInCurrentPage() || FetchNextPage().GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override Task<bool> ReadAsync(CancellationToken cancellationToken) => ReadNextRowInCurrentPage() ? TrueTask : FetchNextPage();

    /// <inheritdoc/>
    public override IEnumerator GetEnumerator()
        => new DbEnumerator(this); // TODO: ???

    /// <inheritdoc/>
    public ReadOnlyCollection<DbColumn> GetColumnSchema()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc/>
    public override async ValueTask DisposeAsync()
    {
        await _pageEnumerator.DisposeAsync().ConfigureAwait(false);
        await _resultSet.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override void Close() => Dispose();

    /// <inheritdoc/>
    public override Task CloseAsync() => DisposeAsync().AsTask();

    /// <inheritdoc/>
    public override T GetFieldValue<T>(int ordinal)
    {
        return base.GetFieldValue<T>(ordinal);
    }

    /// <inheritdoc/>
    public override Type GetProviderSpecificFieldType(int ordinal)
    {
        return base.GetProviderSpecificFieldType(ordinal);
    }

    /// <inheritdoc/>
    public override object GetProviderSpecificValue(int ordinal)
    {
        return base.GetProviderSpecificValue(ordinal);
    }

    /// <inheritdoc/>
    public override int GetProviderSpecificValues(object[] values)
    {
        return base.GetProviderSpecificValues(values);
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing) => DisposeAsync().AsTask().GetAwaiter().GetResult();

    private BinaryTupleReader GetReader(int ordinal, Type type)
    {
        var column = Metadata.Columns[ordinal];

        if (column.Type != type.ToSqlColumnType())
        {
            throw new InvalidCastException($"Column {column.Name} of type {column.Type} can not be cast to {type}.");
        }

        return GetReader();
    }

    private BinaryTupleReader GetReader()
    {
        if (_pageRowCount < 0)
        {
            throw new InvalidOperationException(
                $"No data exists for the row/column. Reading has not started. Call {nameof(ReadAsync)} or {nameof(Read)}.");
        }

        // TODO: Cache tuple reader header somehow?
        // TODO: Deal with rows.
        var reader = _pageEnumerator.Current.GetReader(_pageRowOffset);
        var tupleSpan = reader.ReadBinary();

        return new BinaryTupleReader(tupleSpan, FieldCount);
    }

    private bool ReadNextRowInCurrentPage()
    {
        if (_pageRowCount <= 0 || _pageRowIndex >= _pageRowCount - 1)
        {
            return false;
        }

        _pageRowIndex++;
        _pageRowOffset += _pageRowSize;
        _pageRowSize = _pageEnumerator.Current.GetReader(_pageRowOffset).ReadBinaryHeader();

        return true;
    }

    private async Task<bool> FetchNextPage()
    {
        if (!await _pageEnumerator.MoveNextAsync().ConfigureAwait(false))
        {
            return false;
        }

        ReadFirstRowInCurrentPage();

        return true;

        void ReadFirstRowInCurrentPage()
        {
            var reader = _pageEnumerator.Current.GetReader();

            _pageRowCount = reader.ReadArrayHeader();
            _pageRowOffset = reader.Consumed;
            _pageRowSize = reader.ReadBinaryHeader() + reader.Consumed - _pageRowOffset;
            _pageRowIndex = 0;
        }
    }
}
