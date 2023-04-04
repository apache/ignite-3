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

using System.Data.Common;
using Internal.Common;
using Internal.Sql;

/// <summary>
/// Represents a column within Ignite result set.
/// </summary>
public sealed class IgniteDbColumn : DbColumn
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IgniteDbColumn"/> class.
    /// </summary>
    /// <param name="column">Column.</param>
    /// <param name="ordinal">Column ordinal.</param>
    internal IgniteDbColumn(IColumnMetadata column, int ordinal)
    {
        ColumnMetadata = column;
        ColumnName = column.Name;
        ColumnOrdinal = ordinal;
        DataTypeName = column.Type.ToSqlTypeName();
        DataType = column.Type.ToClrType();
        AllowDBNull = column.Nullable;
        NumericPrecision = column.Precision < 0 ? null : column.Precision;
        NumericScale = column.Scale < 0 ? null : column.Scale;
    }

    /// <summary>
    /// Gets Ignite-specific column metadata.
    /// </summary>
    public IColumnMetadata ColumnMetadata { get; }

    /// <inheritdoc />
    public override string ToString() =>
        new IgniteToStringBuilder(GetType())
            .Append(ColumnName)
            .Append(ColumnOrdinal)
            .Append(DataTypeName)
            .Append(AllowDBNull)
            .Append(NumericPrecision)
            .Append(NumericScale)
            .Append(ColumnMetadata)
            .Build();
}
