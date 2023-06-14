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

namespace Apache.Ignite.Sql;

using System;

/// <summary>
/// Interface to provide update expressions for <see cref="IgniteQueryableExtensions.ExecuteUpdateAsync{T}" />.
/// </summary>
/// <typeparam name="T">Query element type.</typeparam>
public interface IUpdateDescriptor<out T>
{
    /// <summary>
    /// Specifies member update with a constant value.
    /// </summary>
    /// <typeparam name="TProp">Member type.</typeparam>
    /// <param name="selector">Member selector.</param>
    /// <param name="value">New value.</param>
    /// <returns>Update descriptor for chaining.</returns>
    IUpdateDescriptor<T> SetProperty<TProp>(Func<T, TProp> selector, TProp value);

    /// <summary>
    /// Specifies member update with an expression.
    /// </summary>
    /// <typeparam name="TProp">Member type.</typeparam>
    /// <param name="selector">Member selector.</param>
    /// <param name="valueBuilder">New value generator.</param>
    /// <returns>Update descriptor for chaining.</returns>
    IUpdateDescriptor<T> SetProperty<TProp>(Func<T, TProp> selector, Func<T, TProp> valueBuilder);
}
