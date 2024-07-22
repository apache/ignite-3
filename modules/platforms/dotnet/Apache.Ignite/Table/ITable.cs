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

namespace Apache.Ignite.Table
{
    /// <summary>
    /// Table view.
    /// </summary>
    public interface ITable
    {
        /// <summary>
        /// Gets the table name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the record binary view.
        /// </summary>
        public IRecordView<IIgniteTuple> RecordBinaryView { get; }

        /// <summary>
        /// Gets the key-value binary view.
        /// </summary>
        public IKeyValueView<IIgniteTuple, IIgniteTuple> KeyValueBinaryView { get; }

        /// <summary>
        /// Gets the partition manager.
        /// </summary>
        public IPartitionManager PartitionManager { get; }

        /// <summary>
        /// Gets the record view mapped to specified type <typeparamref name="T"/>.
        /// <para />
        /// Table columns will be mapped to properties or fields by name, ignoring case. Any fields are supported,
        /// including private and readonly.
        /// </summary>
        /// <typeparam name="T">Record type.</typeparam>
        /// <returns>Record view.</returns>
        public IRecordView<T> GetRecordView<T>()
            where T : notnull;

        /// <summary>
        /// Gets the record view mapped to specified key and value types.
        /// <para />
        /// Table columns will be mapped to properties or fields by name, ignoring case. Any fields are supported,
        /// including private and readonly.
        /// </summary>
        /// <typeparam name="TK">Key type.</typeparam>
        /// <typeparam name="TV">Value type.</typeparam>
        /// <returns>Key-value view.</returns>
        public IKeyValueView<TK, TV> GetKeyValueView<TK, TV>()
            where TK : notnull;
    }
}
