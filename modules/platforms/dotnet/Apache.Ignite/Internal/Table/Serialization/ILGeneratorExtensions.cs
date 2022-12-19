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
using Proto.BinaryTuple;

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
    /// <param name="columnName">Column name.</param>
    public static void EmitConv(this ILGenerator il, Type from, Type to, string columnName)
    {
        if (from == to)
        {
            return;
        }

        if (!from.IsValueType || !to.IsValueType)
        {
            EmitConvertTo(il, from, to, columnName);
            return;
        }

        var fromUnderlying = Nullable.GetUnderlyingType(from);
        var toUnderlying = Nullable.GetUnderlyingType(to);

        if (fromUnderlying != null && toUnderlying != null)
        {
            // TODO: Emit "if src == null return null".
            throw new NotSupportedException("TODO");
        }

        if (fromUnderlying != null && toUnderlying == null)
        {
            var loc = il.DeclareAndInitLocal(from);
            il.Emit(OpCodes.Stloc, loc);
            il.Emit(OpCodes.Ldloca, loc);
            var unwrapMethod = from.GetMethod("get_Value")!;
            il.Emit(OpCodes.Call, unwrapMethod);
            EmitConvertTo(il, fromUnderlying, to, columnName);
            return;
        }

        if (fromUnderlying == null && toUnderlying != null)
        {
            EmitConvertTo(il, from, to, columnName);
            il.Emit(OpCodes.Call, to.GetConstructor(new[] { toUnderlying })!);

            return;
        }

        // Normal case without nullables.
        EmitConvertTo(il, from, to, columnName);

        static void EmitConvertTo(ILGenerator il, Type from, Type to, string columnName)
        {
            var methodName = "To" + to.Name;
            var method = typeof(Convert).GetMethod(methodName, BindingFlags.Static | BindingFlags.Public, new[] { from });

            if (method == null)
            {
                throw new NotSupportedException($"Conversion from {from} to {to} is not supported (column '{columnName}').");
            }

            il.Emit(OpCodes.Call, method);
        }
    }

    private static double FromNullableIntToDouble(ref BinaryTupleReader reader)
    {
        return Convert.ToDouble(reader.GetIntNullable(0)!.Value);
    }
}
