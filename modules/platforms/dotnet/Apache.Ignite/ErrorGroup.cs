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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Represents a concept of error group. Error group defines a collection of errors that belong to a single semantic component.
    /// Each group can be identified by a name and an integer number that both must be unique across all error groups.
    /// </summary>
    [SuppressMessage(
        "Microsoft.Design",
        "CA1034:NestedTypesShouldNotBeVisible",
        Justification = "Reviewed.")]
    [SuppressMessage(
        "Microsoft.Naming",
        "CA1724:TypeNamesShouldNotMatchNamespaces",
        Justification = "Types are nested, there is no conflict.")]
    public static partial class ErrorGroup
    {
        /// <summary>
        /// Ignite error prefix.
        /// </summary>
        public const string ErrPrefix = "IGN-";

        /// <summary>
        /// Gets error code extracted from the given full error code.
        /// </summary>
        /// <param name="fullCode">Full error code.</param>
        /// <returns>Error code.</returns>
        public static int GetErrorCode(int fullCode) => fullCode & 0xFFFF;

        /// <summary>
        /// Returns group code extracted from the given full error code.
        /// </summary>
        /// <param name="fullCode">Full error code.</param>
        /// <returns>Group code.</returns>
        public static int GetGroupCode(int fullCode) => fullCode >> 16;

        /// <summary>
        /// Gets the full error code from group and error codes.
        /// </summary>
        /// <param name="groupCode">Group code.</param>
        /// <param name="errorCode">Error code.</param>
        /// <returns>Combined code.</returns>
        public static int GetFullCode(int groupCode, int errorCode) => (groupCode << 16) | (errorCode & 0xFFFF);
    }
}
