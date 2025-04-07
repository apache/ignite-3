package org.apache.ignite.internal.compute.executor.platform;

import java.util.concurrent.CompletableFuture;

@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface PlatformComputeConnection {
    CompletableFuture<byte[]> sendMessage(byte[] message);
}
