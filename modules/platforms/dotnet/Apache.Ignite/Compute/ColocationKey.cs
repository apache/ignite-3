namespace Apache.Ignite.Compute;

public record ColocationKey<T>(string TableName, T Key);
