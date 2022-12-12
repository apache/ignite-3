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

namespace Apache.Ignite.Internal.Table.Serialization;

using System;
using System.Reflection;
using System.Reflection.Emit;

/// <summary>
/// Extensions for <see cref="ILGenerator"/>.
/// </summary>
internal static class ILGeneratorExtensions
{
    /// <summary>
    /// Declares a local and initializes with a default value (all fields zero).
    /// </summary>
    /// <param name="il">IL generator.</param>
    /// <param name="type">Type.</param>
    /// <returns>Resulting builder.</returns>
    public static LocalBuilder DeclareAndInitLocal(this ILGenerator il, Type type)
    {
        var local = il.DeclareLocal(type);

        if (type.IsValueType)
        {
            il.Emit(OpCodes.Ldloca_S, local);
            il.Emit(OpCodes.Initobj, type);
        }
        else
        {
            il.Emit(OpCodes.Ldtoken, type);
            il.Emit(OpCodes.Call, ReflectionUtils.GetTypeFromHandleMethod);
            il.Emit(OpCodes.Call, ReflectionUtils.GetUninitializedObjectMethod);
            il.Emit(OpCodes.Stloc, local);
        }

        return local;
    }

    /// <summary>
    /// Emits corresponding Conv op code.
    /// </summary>
    /// <param name="il">IL generator.</param>
    /// <param name="from">Source type.</param>
    /// <param name="to">Target type.</param>
    public static void EmitConv(this ILGenerator il, Type from, Type to)
    {
        if (from == to)
        {
            return;
        }

        var methodName = "To" + to.Name;
        var method = typeof(Convert).GetMethod(methodName, BindingFlags.Static | BindingFlags.Public, new[] { from });

        if (method == null)
        {
            throw new NotSupportedException($"Conversion from {from} to {to} is not supported.");
        }

        il.Emit(OpCodes.Call, method);
    }
}
