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
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Ignite database parameter.
/// </summary>
public sealed class IgniteDbParameter : DbParameter
{
    private string _parameterName = string.Empty;
    private string _sourceColumn = string.Empty;

    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbParameter"/> class.
    /// </summary>
    public IgniteDbParameter()
    {
        // No-op.
    }

    /// <summary>
    /// Gets or sets the Ignite column type.
    /// </summary>
    public ColumnType IgniteColumnType { get; set; } = ColumnType.String;

    /// <inheritdoc />
    [SuppressMessage("ReSharper", "PatternIsRedundant", Justification = "For clarity.")]
    public override DbType DbType
    {
        get => IgniteColumnType switch
        {
            ColumnType.Boolean => DbType.Boolean,
            ColumnType.Int8 => DbType.Byte,
            ColumnType.Int16 => DbType.Int16,
            ColumnType.Int32 => DbType.Int32,
            ColumnType.Int64 => DbType.Int64,
            ColumnType.Float => DbType.Single,
            ColumnType.Double => DbType.Double,
            ColumnType.Decimal => DbType.Decimal,
            ColumnType.String => DbType.String,
            ColumnType.Time => DbType.Time,
            ColumnType.Uuid => DbType.Guid,
            ColumnType.Datetime => DbType.DateTime,
            ColumnType.Date => DbType.Date,
            ColumnType.ByteArray => DbType.Binary,
            ColumnType.Timestamp => DbType.DateTime2,
            ColumnType.Period or ColumnType.Duration
                => throw new NotSupportedException($"Unsupported Ignite column type: {IgniteColumnType}"),
            _ => throw new InvalidOperationException($"Unexpected Ignite column type: {IgniteColumnType}")
        };
        set => IgniteColumnType = value switch
        {
            DbType.Boolean => ColumnType.Boolean,
            DbType.Byte => ColumnType.Int8,
            DbType.Int16 => ColumnType.Int16,
            DbType.Int32 => ColumnType.Int32,
            DbType.Int64 => ColumnType.Int64,
            DbType.Single => ColumnType.Float,
            DbType.Double => ColumnType.Double,
            DbType.Decimal => ColumnType.Decimal,
            DbType.String => ColumnType.String,
            DbType.Time => ColumnType.Time,
            DbType.Guid => ColumnType.Uuid,
            DbType.DateTime => ColumnType.Datetime,
            DbType.Date => ColumnType.Date,
            DbType.Binary => ColumnType.ByteArray,
            DbType.DateTime2 => ColumnType.Timestamp,
            DbType.AnsiString or DbType.Currency or DbType.Object or DbType.SByte or DbType.UInt16 or DbType.UInt32 or DbType.UInt64
                or DbType.VarNumeric or DbType.AnsiStringFixedLength or DbType.StringFixedLength or DbType.Xml
                or DbType.DateTimeOffset
                => throw new NotSupportedException($"Unsupported Db type: {value}"),
            _ => throw new InvalidOperationException($"Unexpected Db type: {value}")
        };
    }

    /// <summary>
    /// Gets or sets the direction of the parameter. Only <see cref="ParameterDirection.Input" /> is supported.
    /// </summary>
    /// <value>The direction of the parameter.</value>
    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
            {
                throw new ArgumentException($"Only ParameterDirection.Input is supported: {value}", nameof(value));
            }
        }
    }

    /// <inheritdoc />
    public override bool IsNullable { get; set; }

    /// <inheritdoc />
    [AllowNull]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value ?? string.Empty;
    }

    /// <inheritdoc />
    [AllowNull]
    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? string.Empty;
    }

    /// <inheritdoc />
    public override object? Value { get; set; }

    /// <inheritdoc />
    public override bool SourceColumnNullMapping { get; set; }

    /// <inheritdoc />
    public override int Size { get; set; }

    /// <inheritdoc />
    public override void ResetDbType() => IgniteColumnType = ColumnType.String;
}
