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

    /// <inheritdoc />
    [SuppressMessage("ReSharper", "PatternIsRedundant", Justification = "For clarity.")]
    public override DbType DbType
    {
        get => throw new NotSupportedException("DbParameter.DbType is not supported by Ignite. Parameter value determines the type.");
        set => throw new NotSupportedException("DbParameter.DbType is not supported by Ignite. Parameter value determines the type.");
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
    public override bool IsNullable
    {
        get => true;
        set => throw new NotSupportedException("DbParameter.IsNullable is not supported by Ignite.");
    }

    /// <inheritdoc />
    [AllowNull]
    public override string ParameterName // TODO: Ignite does not support named parameters - throw?
    {
        get => _parameterName;
        set => _parameterName = value ?? string.Empty;
    }

    /// <inheritdoc />
    [AllowNull]
    public override string SourceColumn // TODO: Not used in Ignite - remove?
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? string.Empty;
    }

    /// <inheritdoc />
    public override object? Value { get; set; }

    /// <inheritdoc />
    public override bool SourceColumnNullMapping { get; set; } // TODO: Not used in Ignite - remove?

    /// <inheritdoc />
    public override int Size { get; set; } // TODO: Not used in Ignite - remove?

    /// <inheritdoc />
    public override void ResetDbType() => IgniteColumnType = ColumnType.String;
}
