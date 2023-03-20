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

namespace Apache.Ignite.Internal.Linq;

using System;
using System.Collections.Generic;
using System.Reflection;

/// <summary>
/// Key target for <see cref="ResultSelectorCacheKey"/> cached delegates. Equality logic is based on constructor and properties.
/// </summary>
public readonly struct MemberInitCacheTarget : IEquatable<MemberInitCacheTarget>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MemberInitCacheTarget"/> struct.
    /// </summary>
    /// <param name="ctorInfo">Target constructor info.</param>
    /// <param name="propertiesOrFields">Target properties of field infos.</param>
    public MemberInitCacheTarget(ConstructorInfo ctorInfo, IReadOnlyList<MemberInfo> propertiesOrFields)
    {
        CtorInfo = ctorInfo;
        PropertiesOrFields = propertiesOrFields;
    }

    /// <summary>
    /// Gets constructor info.
    /// </summary>
    public ConstructorInfo CtorInfo { get; }

    /// <summary>
    /// Gets properties or fields infos.
    /// </summary>
    public IReadOnlyList<MemberInfo> PropertiesOrFields { get; }

    /// <summary>
    /// Test.
    /// </summary>
    /// <param name="left">left.</param>
    /// <param name="right">right.</param>
    /// <returns>bool.</returns>
    public static bool operator ==(MemberInitCacheTarget left, MemberInitCacheTarget right) => left.Equals(right);

    /// <summary>
    /// Test.
    /// </summary>
    /// <param name="left">left.</param>
    /// <param name="right">right.</param>
    /// <returns>bool.</returns>
    public static bool operator !=(MemberInitCacheTarget left, MemberInitCacheTarget right) => !(left == right);

    /// <inheritdoc/>
    public bool Equals(MemberInitCacheTarget other)
    {
        if (!CtorInfo.Equals(other.CtorInfo))
        {
            return false;
        }

        if (PropertiesOrFields.Count != other.PropertiesOrFields.Count)
        {
            return false;
        }

        for (var i = 0; i < PropertiesOrFields.Count; i++)
        {
            if (PropertiesOrFields[i] != other.PropertiesOrFields[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is MemberInitCacheTarget other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        HashCode hash = default;

        hash.Add(CtorInfo);

        foreach (var propertyOrField in PropertiesOrFields)
        {
            hash.Add(propertyOrField);
        }

        return hash.ToHashCode();
    }
}
