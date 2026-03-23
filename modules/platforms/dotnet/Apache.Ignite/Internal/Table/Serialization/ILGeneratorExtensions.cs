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
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;

/// <summary>
/// Extensions for <see cref="ILGenerator"/>.
/// </summary>
[RequiresUnreferencedCode(ReflectionUtils.TrimWarning)]
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

        if (to.IsEnum)
        {
            to = to.GetEnumUnderlyingType();
        }

        if (!from.IsValueType || !to.IsValueType)
        {
            EmitConvertTo(il, from, to, columnName);
            return;
        }

        var fromUnderlying = Nullable.GetUnderlyingType(from);
        var toUnderlying = Nullable.GetUnderlyingType(to);

        if (toUnderlying is { IsEnum: true })
        {
            toUnderlying = toUnderlying.GetEnumUnderlyingType();
            to = typeof(Nullable<>).MakeGenericType(toUnderlying);
        }

        if (fromUnderlying != null && toUnderlying != null)
        {
            var emitNull = il.DefineLabel();
            var end = il.DefineLabel();

            var fromLoc = il.DeclareLocal(from);
            var toLoc = il.DeclareLocal(to);
            il.Emit(OpCodes.Stloc, fromLoc);
            il.Emit(OpCodes.Ldloca, fromLoc);
            il.Emit(OpCodes.Call, from.GetMethod("get_HasValue")!);
            il.Emit(OpCodes.Brfalse, emitNull);

            // Unwrap, convert, wrap.
            il.Emit(OpCodes.Ldloca, fromLoc);
            il.Emit(OpCodes.Call, from.GetMethod("get_Value")!);
            EmitConvertTo(il, fromUnderlying, toUnderlying, columnName);

            il.Emit(OpCodes.Newobj, to.GetConstructor(new[] { toUnderlying })!);
            il.Emit(OpCodes.Br, end);

            // Null.
            il.MarkLabel(emitNull);
            il.Emit(OpCodes.Ldloca, toLoc);
            il.Emit(OpCodes.Initobj, to);
            il.Emit(OpCodes.Ldloc, toLoc);

            // End.
            il.MarkLabel(end);
            return;
        }

        if (fromUnderlying != null && toUnderlying == null)
        {
            var emitNullException = il.DefineLabel();
            var end = il.DefineLabel();

            var fromLoc = il.DeclareLocal(from);
            il.Emit(OpCodes.Stloc, fromLoc);

            // Check null.
            il.Emit(OpCodes.Ldloca, fromLoc);
            il.Emit(OpCodes.Call, from.GetMethod("get_HasValue")!);
            il.Emit(OpCodes.Brfalse, emitNullException);

            // Not null: unwrap.
            il.Emit(OpCodes.Ldloca, fromLoc);
            il.Emit(OpCodes.Call, from.GetMethod("get_Value")!);
            EmitConvertTo(il, fromUnderlying, to, columnName);
            il.Emit(OpCodes.Br, end);

            // Null: throw.
            il.MarkLabel(emitNullException);
            il.Emit(OpCodes.Ldstr, $"Can not read NULL from column '{columnName}' of type '{from}' into type '{to}'.");
            il.Emit(OpCodes.Newobj, typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!);
            il.Emit(OpCodes.Throw);

            il.MarkLabel(end);
            return;
        }

        if (fromUnderlying == null && toUnderlying != null)
        {
            EmitConvertTo(il, from, to, columnName);
            il.Emit(OpCodes.Newobj, to.GetConstructor([toUnderlying])!);

            return;
        }

        // Normal case without nullables.
        EmitConvertTo(il, from, to, columnName);

        static void EmitConvertTo(ILGenerator il, Type from, Type to, string columnName)
        {
            if (from == to)
            {
                return;
            }

            var methodName = "To" + to.Name;
            var method = typeof(Convert).GetMethod(methodName, BindingFlags.Static | BindingFlags.Public, [from]);

            if (method == null)
            {
                throw NotSupportedConversion(from, to, columnName);
            }

            if (from.IsValueType && method.GetParameters()[0].ParameterType == typeof(object))
            {
                if (!from.IsAssignableTo(typeof(IConvertible)))
                {
                    throw NotSupportedConversion(from, to, columnName);
                }

                // Convert.ToX(object).
                il.Emit(OpCodes.Box, from);
            }

            il.Emit(OpCodes.Call, method);
        }

        static NotSupportedException NotSupportedConversion(Type from, Type to, string columnName) =>
            new($"Conversion from {from} to {to} is not supported (column '{columnName}').");
    }
}
