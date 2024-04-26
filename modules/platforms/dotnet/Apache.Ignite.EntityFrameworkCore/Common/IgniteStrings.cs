namespace Apache.Ignite.EntityFrameworkCore.Common;

public static class IgniteStrings
{
    public const string SequencesNotSupported = "Ignite does not support sequences.";
    public const string ApplyNotSupported = "Ignite does not support APPLY.";
    public const string MigrationScriptGenerationNotSupported = "Ignite does not support migration script generation.";

    public static string InvalidMigrationOperation(string shortDisplayName) => "Migration operation is invalid: " + shortDisplayName;
}
