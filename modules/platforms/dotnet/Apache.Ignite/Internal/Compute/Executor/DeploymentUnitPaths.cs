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

namespace Apache.Ignite.Internal.Compute.Executor;

using System;
using System.Collections.Generic;
using System.Text;

/// <summary>
/// Represents a set of deployment unit paths.
/// <para />
/// - A set of deployment unit paths is considered equal if it contains the same paths in the same order.
/// - Unit order affects assembly resolution order.
/// - A given deployment unit (name+ver) is always located at the same path (after it is uploaded to the cluster).
///   Example: ignite-3/target/work/PlatformTestNodeRunner/o.a.i.internal.runner.app.PlatformTestNodeRunner/deployment/UNIT_NAME/UNIT_VER.
/// </summary>
/// <param name="Paths">The list of deployment unit paths.</param>
internal readonly record struct DeploymentUnitPaths(IReadOnlyList<string> Paths)
{
    /// <inheritdoc/>
    public override int GetHashCode()
    {
        HashCode hashCode = default;

        foreach (var path in Paths)
        {
            hashCode.Add(path);
        }

        return hashCode.ToHashCode();
    }

    /// <summary>
    /// Determines whether the specified object instances are considered equal.
    /// </summary>
    /// <param name="other">The object to compare.</param>
    /// <returns>
    /// <see langword="true" /> if the objects are considered equal; otherwise, <see langword="false" />.
    /// </returns>
    public bool Equals(DeploymentUnitPaths other)
    {
        var otherPaths = other.Paths;
        if (Paths.Count != otherPaths.Count)
        {
            return false;
        }

        for (var i = 0; i < Paths.Count; i++)
        {
            if (!Paths[i].Equals(otherPaths[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private bool PrintMembers(StringBuilder builder)
    {
        foreach (var path in Paths)
        {
            builder.Append(path).Append(", ");
        }

        return true;
    }
}
