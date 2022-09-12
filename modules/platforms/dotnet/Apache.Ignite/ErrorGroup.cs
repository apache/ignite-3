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
    using System;
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
    public static class ErrorGroup
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

        /// <summary>
        /// Gets the group name by code.
        /// </summary>
        /// <param name="groupCode">Group code.</param>
        /// <returns>Group name.</returns>
        public static string GetGroupName(int groupCode) => groupCode switch
        {
            Common.GroupCode => "CMN",
            Table.GroupCode => "TBL",
            Client.GroupCode => "CLIENT",
            Sql.GroupCode => "SQL",
            MetaStorage.GroupCode => "META",
            Index.GroupCode => "IDX",
            Transactions.GroupCode => "TX",
            _ => throw new Exception("Unknown error group code: " + groupCode)
        };

        /// <summary> Common errors. </summary>
        public static class Common
        {
            /// <summary> Group code. </summary>
            public const int GroupCode = 1;

            /// <summary> Unexpected error. </summary>
            public static readonly int Unexpected = GetFullCode(GroupCode, 1);

            /// <summary> Node stopping error. </summary>
            public static readonly int NodeStopping = GetFullCode(GroupCode, 2);

            /// <summary> Component not started error. </summary>
            public static readonly int ComponentNotStarted = GetFullCode(GroupCode, 3);

            /// <summary> Unknown error. </summary>
            public static readonly int Unknown = GetFullCode(GroupCode, 0xFFFF);
        }

        /// <summary> Table errors. </summary>
        public static class Table
        {
            /// <summary> Group code. </summary>
            public const int GroupCode = 2;

            /// <summary> Table already exists. </summary>
            public static readonly int TableAlreadyExists = GetFullCode(GroupCode, 1);

            /// <summary> Table not found. </summary>
            public static readonly int TableNotFound = GetFullCode(GroupCode, 2);

            /// <summary> Column already exists. </summary>
            public static readonly int ColumnAlreadyExists = GetFullCode(GroupCode, 3);

            /// <summary> Column not found. </summary>
            public static readonly int ColumnNotFound = GetFullCode(GroupCode, 4);
        }

        /// <summary> Client errors. </summary>
        public static class Client
        {
            /// <summary> Group code. </summary>
            public const int GroupCode = 3;

            /// <summary> Connection failed. </summary>
            public static readonly int Connection = GetFullCode(GroupCode, 1);

            /// <summary> Protocol breakdown. </summary>
            public static readonly int Protocol = GetFullCode(GroupCode, 2);

            /// <summary> Incompatible protocol version. </summary>
            public static readonly int ProtocolCompatibility = GetFullCode(GroupCode, 3);

            /// <summary> Table not found by ID. </summary>
            public static readonly int TableIdNotFound = GetFullCode(GroupCode, 4);

            /// <summary> Authentication error. </summary>
            public static readonly int Authentication = GetFullCode(GroupCode, 5);

            /// <summary> Authorization error. </summary>
            public static readonly int Authorization = GetFullCode(GroupCode, 6);

            /// <summary> Configuration error. </summary>
            public static readonly int Configuration = GetFullCode(GroupCode, 7);
        }

        /// <summary> SQL errors. </summary>
        public static class Sql
        {
            /// <summary> Group code. </summary>
            public const int GroupCode = 4;

            /// <summary> No more pages in the cursor error. </summary>
            public static readonly int CursorNoMorePages = GetFullCode(GroupCode, 1);

            /// <summary> Session not found error. </summary>
            public static readonly int SessionNotFound = GetFullCode(GroupCode, 2);

            /// <summary> Invalid query error. </summary>
            public static readonly int QueryInvalid = GetFullCode(GroupCode, 3);

            /// <summary> Query without a result set error. </summary>
            public static readonly int QueryNoResultSet = GetFullCode(GroupCode, 4);

            /// <summary> Missing primary key error. </summary>
            public static readonly int PrimaryKeyMissing = GetFullCode(GroupCode, 5);

            /// <summary> Multiple primary keys error. </summary>
            public static readonly int PrimaryKeysMultiple = GetFullCode(GroupCode, 6);

            /// <summary> Schema not found. </summary>
            public static readonly int SchemaNotFound = GetFullCode(GroupCode, 7);

            /// <summary> Storage engine not valid. </summary>
            public static readonly int StorageEngineNotValid = GetFullCode(GroupCode, 8);

            /// <summary> Cursor is already closed error. </summary>
            public static readonly int CursorClosed = GetFullCode(GroupCode, 9);

            /// <summary> Some keys can't be inserted because they violates unique constraint PK. </summary>
            public static readonly int DuplicateKeys = GetFullCode(GroupCode, 10);
        }

        /// <summary> Meta storage errors. </summary>
        public static class MetaStorage
        {
            /// <summary> Group code. </summary>
            public const int GroupCode = 5;
        }

        /// <summary> Indexing errors. </summary>
        public static class Index
        {
            /// <summary> Group code. </summary>
            public const int GroupCode = 6;

            // TODO: Use source generators for this!
        }

        /// <summary> Transaction errors. </summary>
        public static class Transactions
        {
            /// <summary> Group code. </summary>
            public const int GroupCode = 7;
        }
    }
}
