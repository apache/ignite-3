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
using System.Globalization;
using System.Numerics;
using Internal.Proto.BinaryTuple;

/// <summary>
/// Big decimal.
/// <para />
/// Ignite supports values with up to <see cref="short.MaxValue"/> precision (in tables, SQL, Compute, and other APIs).
/// .NET <see cref="decimal"/> has 28-29 digit precision and can not represent all values that Ignite supports.
/// This type fills the gap.
/// </summary>
public readonly record struct BigDecimal : IComparable<BigDecimal>, IComparable, IFormattable
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BigDecimal"/> struct.
    /// </summary>
    /// <param name="unscaledValue">Unscaled value.</param>
    /// <param name="scale">Scale.</param>
    public BigDecimal(BigInteger unscaledValue, short scale)
    {
        UnscaledValue = unscaledValue;
        Scale = scale;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="BigDecimal"/> struct.
    /// </summary>
    /// <param name="value">Decimal value.</param>
    public BigDecimal(decimal value) =>
        (UnscaledValue, Scale) = BinaryTupleCommon.DecimalToUnscaledBigInteger(value, maxScale: short.MaxValue);

    /// <summary>
    /// Gets the unscaled value.
    /// </summary>
    public BigInteger UnscaledValue { get; }

    /// <summary>
    /// Gets the scale.
    /// </summary>
    public short Scale { get; }

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

    public static bool operator <(BigDecimal left, BigDecimal right) => left.CompareTo(right) < 0;

    public static bool operator <=(BigDecimal left, BigDecimal right) => left.CompareTo(right) <= 0;

    public static bool operator >(BigDecimal left, BigDecimal right) => left.CompareTo(right) > 0;

    public static bool operator >=(BigDecimal left, BigDecimal right) => left.CompareTo(right) >= 0;

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member

    /// <summary>
    /// Converts the value of this instance to its equivalent decimal representation.
    /// </summary>
    /// <returns>Decimal representation of the current value.</returns>
    /// <exception cref="OverflowException">Value was either too large or too small for a Decimal.</exception>
    public decimal ToDecimal()
    {
        decimal res = (decimal)UnscaledValue;

        if (Scale > 0)
        {
            res /= (decimal)BigInteger.Pow(10, Scale);
        }
        else if (Scale < 0)
        {
            res *= (decimal)BigInteger.Pow(10, -Scale);
        }

        return res;
    }

    /// <inheritdoc />
    public int CompareTo(BigDecimal other)
    {
        if (Equals(this, other))
        {
            return 0;
        }

        if (Scale == other.Scale)
        {
            return UnscaledValue.CompareTo(other.UnscaledValue);
        }

        if (Scale > other.Scale)
        {
            var otherVal = other.UnscaledValue * BigInteger.Pow(10, Scale - other.Scale);
            return UnscaledValue.CompareTo(otherVal);
        }

        var thisVal = UnscaledValue * BigInteger.Pow(10, other.Scale - Scale);
        return thisVal.CompareTo(other.UnscaledValue);
    }

    /// <inheritdoc />
    public int CompareTo(object? obj)
    {
        if (obj is null)
        {
            return 1;
        }

        if (obj is BigDecimal other)
        {
            return CompareTo(other);
        }

        throw new ArgumentException($"Unexpected object type: {obj.GetType()}. Object must be of type {nameof(BigDecimal)}.");
    }

    /// <inheritdoc />
    public override string ToString() => ToString(null);

    /// <inheritdoc />
    public string ToString(string? format, IFormatProvider? formatProvider)
    {
        if (format != null)
        {
            throw new NotSupportedException("Format string is not supported.");
        }

        return ToString(formatProvider);
    }

    /// <summary>
    /// Converts the numeric value of this object to its equivalent string representation.
    /// </summary>
    /// <param name="provider">An object that supplies culture-specific formatting information.</param>
    /// <returns>The string representation of the current value.</returns>
    public string ToString(IFormatProvider? provider)
    {
        var numberFormatInfo = NumberFormatInfo.GetInstance(provider);

        var res = UnscaledValue.ToString("D", provider);

        if (Scale == 0 || res == "0")
        {
            return res;
        }

        if (Scale < 0)
        {
            return res + new string('0', -Scale);
        }

        res = res.PadLeft(Scale, '0');

        if (res.Length == Scale)
        {
            return "0." + res;
        }

        return res.Insert(res.Length - Scale, numberFormatInfo.NumberDecimalSeparator);
    }
}
