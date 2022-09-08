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

namespace Apache.Ignite
{
    using System.Collections.Generic;

    /// <summary>
    /// Represents a concept of error group. Error group defines a collection of errors that belong to a single semantic component.
    /// Each group can be identified by a name and an integer number that both must be unique across all error groups.
    /// </summary>
    public class ErrorGroup
    {
        /// <summary>
        /// Ignite error prefix.
        /// </summary>
        public const string ErrPrefix = "IGN-";

        /** Contains error codes for this error group. */
        private readonly HashSet<int> _codes = new();

        /// <summary>
        /// Gets the group name.
        /// </summary>
        public string GroupName { get; }

        /// <summary>
        /// Gets the group code.
        /// </summary>
        public int GroupCode { get; }

        /// <summary>
        /// Returns error code extracted from the given full error code.
        /// </summary>
        /// <param name="code">Full error code.</param>
        /// <returns>Error code.</returns>
        public static int ExtractErrorCode(int code) => code & 0xFFFF;
    }
}
