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

namespace Apache.Ignite.Table;

/// <summary>
/// Data streamer item.
/// </summary>
/// <param name="Data">Data item.</param>
/// <param name="OperationType">Operation type.</param>
/// <typeparam name="T">Data type.</typeparam>
public record struct DataStreamerItem<T>(
    T Data,
    DataStreamerOperationType OperationType);

/// <summary>
/// Creates instances of the <see cref="DataStreamerItem{T}"/> struct.
/// </summary>
public static class DataStreamerItem
{
    /// <summary>
    /// Creates a new data streamer item with the <see cref="DataStreamerOperationType.Put"/> operation type.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <typeparam name="T">Data type.</typeparam>
    /// <returns>Data streamer item.</returns>
    public static DataStreamerItem<T> Create<T>(T data) =>
        new(data, DataStreamerOperationType.Put);

    /// <summary>
    /// Creates a new data streamer item instance using provided values.
    /// </summary>
    /// <param name="data">Data.</param>
    /// <param name="operationType">Operation type.</param>
    /// <typeparam name="T">Data type.</typeparam>
    /// <returns>Data streamer item.</returns>
    public static DataStreamerItem<T> Create<T>(T data, DataStreamerOperationType operationType) =>
        new(data, operationType);
}
