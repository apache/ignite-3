namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using Microsoft.EntityFrameworkCore.Storage;

public class IgniteParameterNameGenerator : ParameterNameGenerator
{
    private int _count;

    public override string GenerateNext()
        => "?" + _count++;

    public override void Reset()
        => _count = 0;
}
