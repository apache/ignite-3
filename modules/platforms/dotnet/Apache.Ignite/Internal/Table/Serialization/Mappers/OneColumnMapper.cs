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

namespace Apache.Ignite.Internal.Table.Serialization.Mappers;

using System.Diagnostics.CodeAnalysis;
using Apache.Ignite.Table.Mapper;

/// <summary>
/// One column mapper. Maps the first schema column to a simple type.
/// The rest of the columns are ignored (consistent with existing <see cref="ObjectSerializerHandler{T}"/> behavior).
/// </summary>
/// <typeparam name="T">Type.</typeparam>
[SuppressMessage("MaintainabilityRules", "SA1402:File may only contain a single type", Justification = "Reviewed.")]
internal sealed record OneColumnMapper<T>(MapperReader<T> Reader, MapperWriter<T> Writer) : IMapper<T>
{
    /// <inheritdoc/>
    public void Write(T obj, ref RowWriter rowWriter, IMapperSchema schema)
    {
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            if (i == 0)
            {
                Writer(obj, ref rowWriter, schema);
            }
            else
            {
                // Every column must be handled (written or skipped).
                rowWriter.Skip();
            }
        }
    }

    /// <inheritdoc/>
    public T Read(ref RowReader rowReader, IMapperSchema schema)
    {
        return Reader(ref rowReader, schema);
    }
}
