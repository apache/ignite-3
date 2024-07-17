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

namespace Apache.Ignite.Internal.Linq;

using System.Linq;

/// <summary>
/// Options for <see cref="ResultSelector"/>.
/// </summary>
internal enum ResultSelectorOptions
{
    /// <summary>
    /// None.
    /// </summary>
    None,

    /// <summary>
    /// Whether to read null values as default for value types
    /// (when <see cref="Queryable.DefaultIfEmpty{TSource}(System.Linq.IQueryable{TSource})"/> is used).
    /// </summary>
    ReturnDefaultIfNull,

    /// <summary>
    /// Whether to read null values as zero for nullable value types.
    /// </summary>
    ReturnZeroIfNull,

    /// <summary>
    /// Whether to throw an exception if the result is null.
    /// </summary>
    ThrowNoElementsIfNull
}
