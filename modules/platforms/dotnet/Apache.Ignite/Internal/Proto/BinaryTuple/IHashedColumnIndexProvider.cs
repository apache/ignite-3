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

namespace Apache.Ignite.Internal.Proto.BinaryTuple;

/// <summary>
/// Provides a value indicating whether the column at specified index should be included in the hash.
/// </summary>
internal interface IHashedColumnIndexProvider
{
    /// <summary>
    /// Gets the number of hashed columns.
    /// </summary>
    int HashedColumnCount { get; }

    /// <summary>
    /// Gets a value indicating the hash order for the column at specified index.
    /// </summary>
    /// <param name="index">Column index.</param>
    /// <returns>The order of the column within the hash, when applicable; -1 otherwise.</returns>
    int HashedColumnOrder(int index);
}
