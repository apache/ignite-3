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
using System.Diagnostics.CodeAnalysis;
using System.Text;

/// <summary>
/// A wrapper that may or may not contain a value of type <typeparamref name="T"/>.
/// </summary>
/// <typeparam name="T">Value type.</typeparam>
[SuppressMessage("Microsoft.Naming", "CA1716:IdentifiersShouldNotMatchKeywords", Justification = "Reviewed.")]
public readonly record struct Option<T>
{
    private readonly T _value;

    /// <summary>
    /// Initializes a new instance of the <see cref="Option{T}"/> struct.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="hasValue">Whether the value is present.</param>
    [SuppressMessage(
        "StyleCop.CSharp.DocumentationRules",
        "SA1642:ConstructorSummaryDocumentationMustBeginWithStandardText",
        Justification = "False positive.")]
    internal Option(T value, bool hasValue)
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
    /// Deconstructs this instance.
    /// </summary>
    /// <param name="value">Value.</param>
    /// <param name="hasValue">Whether the value is present.</param>
    public void Deconstruct(out T value, out bool hasValue)
    {
        value = _value;
        hasValue = HasValue;
    }

    /// <summary>
    /// Maps this instance to another type.
    /// </summary>
    /// <param name="selector">Selector.</param>
    /// <typeparam name="TRes">Result type.</typeparam>
    /// <returns>Resulting option.</returns>
    public Option<TRes> Select<TRes>(Func<T, TRes> selector) => HasValue && selector != null! ? Option.Some(selector(_value)) : default!;

    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append("HasValue = ");
        builder.Append(HasValue);

        if (HasValue)
        {
            builder.Append(", Value = ");
            builder.Append(_value);
        }

        return true;
    }
}

/// <summary>
/// Static helpers for <see cref="Option{T}"/>.
/// </summary>
[SuppressMessage("Microsoft.Naming", "CA1716:IdentifiersShouldNotMatchKeywords", Justification = "Reviewed.")]
public static class Option
{
    /// <summary>
    /// Returns an option of the specified value.
    /// </summary>
    /// <param name="val">Value.</param>
    /// <typeparam name="T">value type.</typeparam>
    /// <returns>Option of T.</returns>
    public static Option<T> Some<T>(T val) => new(val, true);

    /// <summary>
    /// Returns an option without a value.
    /// </summary>
    /// <typeparam name="T">value type.</typeparam>
    /// <returns>Option of T.</returns>
    public static Option<T> None<T>() => default;
}
