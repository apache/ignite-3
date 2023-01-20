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
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Internal.Buffers;
using Internal.Common;
using Internal.Proto;
using Internal.Proto.BinaryTuple;
using Internal.Sql;
using NodaTime;

/// <summary>
/// Reads a forward-only stream of rows from an Ignite result set.
/// </summary>
[SuppressMessage("Design", "CA1010:Generic interface should also be implemented", Justification = "Generic IEnumerable is not applicable.")]
[SuppressMessage("Usage", "CA2215:Dispose methods should call base class dispose", Justification = "Base class dispose is empty.")]
public sealed class IgniteDbDataReader : DbDataReader, IDbColumnSchemaGenerator
{
    // TODO: A method to get bytes into a span. And to get the size?
    // TODO: Methods to read Ignite-specific types.
    private static readonly Task<bool> TrueTask = Task.FromResult(true);

    private readonly ResultSet<object> _resultSet;

    private readonly IAsyncEnumerator<PooledBuffer> _pageEnumerator;

    private int _pageRowCount = -1;

    private int _pageRowIndex = -1;

    private int _pageRowOffset = -1;

    private int _pageRowSize = -1;

    private ReadOnlyCollection<DbColumn>? _schema;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbDataReader"/> class.
    /// </summary>
    /// <param name="resultSet">Result set.</param>
    internal IgniteDbDataReader(ResultSet<object> resultSet)
    {
        Debug.Assert(resultSet.HasRowSet, "_resultSet.HasRowSet");

        _resultSet = resultSet;

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
    public override object this[int ordinal] => GetValue(ordinal);

    /// <inheritdoc/>
    public override object this[string name] => GetValue(Metadata.IndexOf(name));

    /// <inheritdoc />
    public override bool GetBoolean(int ordinal) => GetReader(ordinal, typeof(bool)).GetByteAsBool(ordinal);

    /// <inheritdoc/>
    public override byte GetByte(int ordinal) => Metadata.Columns[ordinal] switch
    {
        var c when c.Type.IsAnyInt() => unchecked((byte)GetReader().GetByte(ordinal)),
        var c => throw GetInvalidColumnTypeException(typeof(byte), c)
    };

    /// <inheritdoc/>
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset is < 0 or > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(dataOffset),
                dataOffset,
                $"{nameof(dataOffset)} must be between {0} and {int.MaxValue}");
        }

        if (buffer != null && (bufferOffset < 0 || bufferOffset >= buffer.Length + 1))
        {
            throw new ArgumentOutOfRangeException($"{nameof(bufferOffset)} must be between {0} and {(buffer.Length)}");
        }

        if (buffer != null && (length < 0 || length > buffer.Length - bufferOffset))
        {
            throw new ArgumentOutOfRangeException($"{nameof(length)} must be between {0} and {buffer.Length - bufferOffset}");
        }

        var span = GetReader(ordinal, typeof(byte[])).GetBytesSpan(ordinal);

        if (buffer == null)
        {
            return span.Length;
        }

        var slice = span.Slice(checked((int)dataOffset), length);
        slice.CopyTo(buffer);

        return slice.Length;
    }

    /// <inheritdoc/>
    public override char GetChar(int ordinal) => throw new NotSupportedException("char data type is not supported");

    /// <inheritdoc/>
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset is < 0 or > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(dataOffset),
                dataOffset,
                $"{nameof(dataOffset)} must be between {0} and {int.MaxValue}");
        }

        if (buffer != null && (bufferOffset < 0 || bufferOffset >= buffer.Length + 1))
        {
            throw new ArgumentOutOfRangeException($"{nameof(bufferOffset)} must be between {0} and {(buffer.Length)}");
        }

        if (buffer != null && (length < 0 || length > buffer.Length - bufferOffset))
        {
            throw new ArgumentOutOfRangeException($"{nameof(length)} must be between {0} and {buffer.Length - bufferOffset}");
        }

        var span = GetReader(ordinal, typeof(byte[])).GetBytesSpan(ordinal);

        if (buffer == null)
        {
            return ProtoCommon.StringEncoding.GetCharCount(span);
        }

        return ProtoCommon.StringEncoding.GetChars(
            span.Slice(checked((int)dataOffset)),
            buffer.AsSpan().Slice(bufferOffset, length));
    }

    /// <inheritdoc/>
    public override string GetDataTypeName(int ordinal) => Metadata.Columns[ordinal].Type.ToString();

    /// <inheritdoc/>
    public override DateTime GetDateTime(int ordinal)
    {
        var column = Metadata.Columns[ordinal];

        return column.Type switch
        {
            SqlColumnType.Date => GetReader().GetDate(ordinal).ToDateTimeUnspecified(),
            SqlColumnType.Datetime => GetReader().GetDateTime(ordinal).ToDateTimeUnspecified(),
            SqlColumnType.Timestamp => GetReader().GetTimestamp(ordinal).ToDateTimeUtc(),
            _ => throw GetInvalidColumnTypeException(typeof(DateTime), column)
        };
    }

    /// <inheritdoc/>
    public override decimal GetDecimal(int ordinal)
    {
        var column = Metadata.Columns[ordinal];

        ValidateColumnType(typeof(decimal), column);

        return GetReader().GetDecimal(ordinal, column.Scale);
    }

    /// <inheritdoc/>
    public override double GetDouble(int ordinal) => Metadata.Columns[ordinal] switch
    {
        var c when c.Type.IsAnyFloat() => GetReader().GetDouble(ordinal),
        var c => throw GetInvalidColumnTypeException(typeof(double), c)
    };

    /// <inheritdoc/>
    public override float GetFloat(int ordinal) => Metadata.Columns[ordinal] switch
    {
        var c when c.Type.IsAnyFloat() => GetReader().GetFloat(ordinal),
        var c => throw GetInvalidColumnTypeException(typeof(double), c)
    };

    /// <inheritdoc/>
    public override Guid GetGuid(int ordinal) => GetReader(ordinal, typeof(Guid)).GetGuid(ordinal);

    /// <inheritdoc/>
    public override short GetInt16(int ordinal) => Metadata.Columns[ordinal] switch
    {
        var c when c.Type.IsAnyInt() => GetReader().GetShort(ordinal),
        var c => throw GetInvalidColumnTypeException(typeof(short), c)
    };

    /// <inheritdoc/>
    public override int GetInt32(int ordinal) => Metadata.Columns[ordinal] switch
    {
        var c when c.Type.IsAnyInt() => GetReader().GetInt(ordinal),
        var c => throw GetInvalidColumnTypeException(typeof(int), c)
    };

    /// <inheritdoc/>
    public override long GetInt64(int ordinal) => Metadata.Columns[ordinal] switch
    {
        var c when c.Type.IsAnyInt() => GetReader().GetLong(ordinal),
        var c => throw GetInvalidColumnTypeException(typeof(long), c)
    };

    /// <inheritdoc/>
    public override string GetName(int ordinal) => Metadata.Columns[ordinal].Name;

    /// <inheritdoc/>
    public override int GetOrdinal(string name) => Metadata.IndexOf(name);

    /// <inheritdoc/>
    public override string GetString(int ordinal) => GetReader(ordinal, typeof(string)).GetString(ordinal);

    /// <inheritdoc/>
    public override object GetValue(int ordinal)
    {
        var reader = GetReader();

        return Sql.ReadColumnValue(ref reader, Metadata.Columns[ordinal], ordinal)!;
    }

    /// <inheritdoc/>
    public override int GetValues(object[] values)
    {
        IgniteArgumentCheck.NotNull(values, nameof(values));

        var cols = Metadata.Columns;
        var count = Math.Min(values.Length, cols.Count);

        var reader = GetReader();

        for (int i = 0; i < count; i++)
        {
            values[i] = Sql.ReadColumnValue(ref reader, cols[i], i)!;
        }

        return count;
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
    public override IEnumerator GetEnumerator() => new DbEnumerator(this);

    /// <inheritdoc/>
    public ReadOnlyCollection<DbColumn> GetColumnSchema()
    {
        if (_schema == null)
        {
            var schema = new List<DbColumn>(FieldCount);

            foreach (var col in Metadata.Columns)
            {
                schema.Add(new IgniteDbColumn(col));
            }

            _schema = schema.AsReadOnly();
        }

        return _schema;
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
        // TODO: This should be flexible and support standard types like DateTime and also provider-specific DateOnly etc.
        // TODO: Support all types.
        if (typeof(T) == typeof(string))
        {
            return (T)(object)GetString(ordinal);
        }

        if (typeof(T) == typeof(int))
        {
            return (T)(object)GetInt32(ordinal);
        }

        if (typeof(T) == typeof(LocalTime))
        {
            return (T)(object)GetReader(ordinal, typeof(LocalTime)).GetTime(ordinal);
        }

        throw GetInvalidColumnTypeException(typeof(T), Metadata.Columns[ordinal]);
    }

    /// <inheritdoc/>
    public override Type GetFieldType(int ordinal) => Metadata.Columns[ordinal].Type.ToClrType();

    /// <inheritdoc/>
    protected override void Dispose(bool disposing) => DisposeAsync().AsTask().GetAwaiter().GetResult();

    private static void ValidateColumnType(Type type, IColumnMetadata column)
    {
        if (column.Type != type.ToSqlColumnType())
        {
            throw GetInvalidColumnTypeException(type, column);
        }
    }

    private static InvalidCastException GetInvalidColumnTypeException(Type type, IColumnMetadata column) =>
        new($"Column {column.Name} of type {column.Type} ({column.Type.ToClrType()}) can not be cast to {type}.");

    private BinaryTupleReader GetReader(int ordinal, Type type)
    {
        var column = Metadata.Columns[ordinal];

        ValidateColumnType(type, column);

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
        // TODO: Benchmark this.
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
