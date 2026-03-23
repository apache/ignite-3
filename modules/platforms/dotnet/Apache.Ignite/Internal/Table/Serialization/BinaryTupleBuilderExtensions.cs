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

namespace Apache.Ignite.Internal.Table.Serialization
{
    using System;
    using Proto.BinaryTuple;

    /// <summary>
    /// Extensions for <see cref="BinaryTupleBuilder"/>.
    /// </summary>
    internal static class BinaryTupleBuilderExtensions
    {
        /// <summary>
        /// Appends a no-value marker.
        /// </summary>
        /// <param name="builder">Builder.</param>
        /// <param name="noValueSet">No-value bit set.</param>
        public static void AppendNoValue(this ref BinaryTupleBuilder builder, scoped Span<byte> noValueSet)
        {
            noValueSet.SetBit(builder.ElementIndex);
            builder.AppendNull();
        }
    }
}
