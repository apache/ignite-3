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
/// Mapper writer.
/// </summary>
public interface IMapperWriter
{
    /// <summary>
    /// Writes the specified object to an Ignite table row.
    /// This method must be called for every column in the schema, in the order of the columns (see <see cref="IMapper{T}.Write"/>).
    /// </summary>
    /// <param name="obj">Object.</param>
    /// <typeparam name="T">Object type.</typeparam>
    void Write<T>(T obj);
}
