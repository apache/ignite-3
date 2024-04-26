namespace Apache.Ignite.EntityFrameworkCore.Common;

using System;
using JetBrains.Annotations;

public class Check
{
    // TODO: Replace with IgniteArgumentCheck via InternalsVisibleTo.
    public static T NotNull<T>([NoEnumeration] T? value, [InvokerParameterName] string parameterName)
    {
        if (value is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        return value;
    }
}
