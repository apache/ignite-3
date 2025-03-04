package org.apache.ignite.internal.client;

import org.jetbrains.annotations.Nullable;

public class WriteContext {
    public @Nullable PartitionMapping pm;
    public @Nullable Long enlistmentToken;
}
