package org.apache.ignite.internal.table.distributed.replicator;

import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;

public class TraceableFuture<T> extends CompletableFuture<T> {
    private StringWriter log = new StringWriter();

    public synchronized void log(String msg) {
        log.append("<" + msg + ">");
    }

    public String message() {
        String str;
        synchronized (this) {
            str = log.toString();
        }
        return str;
    }
}
