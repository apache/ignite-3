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

namespace Apache.Ignite.Table.Mapper;

/// <summary>
/// Maps rows to objects of type <typeparamref name="T"/> and vice versa.
/// <para />
/// This API is performance-oriented and mostly intended for use by source generators, but can also enable advanced mapping scenarios.
/// </summary>
/// <typeparam name="T">Mapped object type.</typeparam>
public interface IMapper<T>
{
    /// <summary>
    /// Writes the specified object to an Ignite row.
    /// <para />
    /// The columns must be written in the order defined by the schema. A typical implementation looks like this:
    /// <code>
    /// foreach (var column in schema.Columns)
    /// {
    ///     switch (column.Name)
    ///     {
    ///         case "ID":
    ///             rowWriter.WriteInt(obj.Id);
    ///             break;
    ///         case "NAME":
    ///             rowWriter.WriteString(obj.Name);
    ///             break;
    ///         default:
    ///             rowWriter.Skip(); // Unmapped column.
    ///             break;
    ///     }
    /// }
    /// </code>
    /// Alternatively, if we know the exact schema at compile time, we can write directly:
    /// <code>
    /// rowWriter.WriteInt(obj.Id);
    /// rowWriter.WriteString(obj.Name);
    /// </code>
    /// </summary>
    /// <param name="obj">Object.</param>
    /// <param name="rowWriter">Row writer.</param>
    /// <param name="schema">Row schema.</param>
    void Write(T obj, ref RowWriter rowWriter, IMapperSchema schema);

    /// <summary>
    /// Reads an object of type <typeparamref name="T"/> from an Ignite row.
    /// <para />
    /// The columns must be read in the order defined by the schema. A typical implementation looks like this:
    /// <code>
    /// var obj = new MyObject();
    /// foreach (var column in schema.Columns)
    /// {
    ///     switch (column.Name)
    ///     {
    ///         case "ID":
    ///             obj.Id = rowReader.ReadInt()!.Value;
    ///             break;
    ///         case "NAME":
    ///             obj.Name = rowReader.ReadString();
    ///             break;
    ///         default:
    ///             rowReader.Skip(); // Unmapped column.
    ///             break;
    ///     }
    /// }
    /// return obj;
    /// </code>
    /// Alternatively, if we know the exact schema at compile time, we can read directly:
    /// <code>
    /// return new MyObject
    /// {
    ///     Id = rowReader.ReadInt()!.Value,
    ///     Name = rowReader.ReadString()
    /// };
    /// </code>
    /// </summary>
    /// <param name="rowReader">Row reader.</param>
    /// <param name="schema">Row schema.</param>
    /// <returns>Object.</returns>
    T Read(ref RowReader rowReader, IMapperSchema schema);
}
