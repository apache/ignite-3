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

using System;

/// <summary>
/// Query execution options.
/// </summary>
[Flags]
internal enum ExecutionOptions
{
    /// <summary>
    /// No options.
    /// </summary>
    None = 0,

    /// <summary>
    /// Whether to return a default value when result set is empty.
    /// </summary>
    ReturnDefaultWhenEmpty = 1,

    /// <summary>
    /// Whether to throw "Sequence contains no elements" exception when the result is a single null element.
    /// <para />
    /// SQL MIN/MAX aggregate functions return null when there are no rows in the table,
    /// but LINQ Min/Max throw <see cref="InvalidOperationException"/> in this case.
    /// </summary>
    ThrowNoElementsOnNull = 2,
}
