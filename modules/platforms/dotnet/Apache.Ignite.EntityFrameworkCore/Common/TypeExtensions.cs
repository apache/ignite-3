namespace Apache.Ignite.EntityFrameworkCore.Common;

using System;

internal static class TypeExtensions
{
    public static Type UnwrapNullableType(this Type type)
        => Nullable.GetUnderlyingType(type) ?? type;

    public static bool IsInteger(this Type type)
    {
        type = type.UnwrapNullableType();

        return type == typeof(int)
            || type == typeof(long)
            || type == typeof(short)
            || type == typeof(byte)
            || type == typeof(uint)
            || type == typeof(ulong)
            || type == typeof(ushort)
            || type == typeof(sbyte)
            || type == typeof(char);
    }
}
