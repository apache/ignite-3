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

namespace Apache.Ignite;

using System;

// TODO: Make sure this does not clash with F# option.
// TODO: Convenience methods and constructors, implicit casts.
/// <summary>
/// A wrapper that may or may not contain a value of type <see cref="T"/>.
/// </summary>
/// <typeparam name="T">Value type.</typeparam>
public readonly record struct Option<T>
{
    private readonly T _value;

    #pragma warning disable SA1642
    /// <summary>
    /// Initializes a new instance of the <see cref="Option{T}"/> struct.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="hasValue">Whether the value is present.</param>
    private Option(T value, bool hasValue)
    {
        _value = value;
        HasValue = hasValue;
    }

    /// <summary>
    /// Gets the value.
    /// </summary>
    public T Value => HasValue
        ? _value
        : throw new InvalidOperationException("Value is not present. Check HasValue property before accessing Value.");

    /// <summary>
    /// Gets a value indicating whether the value is present.
    /// </summary>
    public bool HasValue { get; }

    /// <summary>
    /// Wraps a value into an option.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <returns>Wrapped value.</returns>
    public static implicit operator Option<T>(T value) => new(value, true);

    /// <summary>
    /// Deconstructs this instance.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="hasValue">Whether the value is present.</param>
    public void Deconstruct(out T value, out bool hasValue)
    {
        value = _value;
        hasValue = HasValue;
    }
}