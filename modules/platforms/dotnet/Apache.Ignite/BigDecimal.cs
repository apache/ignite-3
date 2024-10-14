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

/// <summary>
/// Big decimal.
/// </summary>
public record struct BigDecimal(BigInteger Value, int Scale) : IComparable<BigDecimal>, IComparable
{
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
    public decimal ToDecimal()
    {
        decimal res = (decimal)Value;

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
            return Value.CompareTo(other.Value);
        }

        if (Scale > other.Scale)
        {
            var otherVal = other.Value * BigInteger.Pow(10, Scale - other.Scale);
            return Value.CompareTo(otherVal);
        }

        var thisVal = Value * BigInteger.Pow(10, other.Scale - Scale);
        return thisVal.CompareTo(other.Value);
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

    /// <summary>
    /// Converts the numeric value of this object to its equivalent string representation.
    /// </summary>
    /// <param name="provider">An object that supplies culture-specific formatting information.</param>
    /// <returns>The string representation of the current value.</returns>
    public string ToString(IFormatProvider? provider)
    {
        var numberFormatInfo = NumberFormatInfo.GetInstance(provider);

        var res = Value.ToString("D", provider);

        return Scale == 0
            ? res
            : res.Insert(Scale, numberFormatInfo.NumberDecimalSeparator);
    }
}
