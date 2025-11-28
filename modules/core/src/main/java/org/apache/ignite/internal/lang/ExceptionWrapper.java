package org.apache.ignite.internal.lang;

import java.io.Serializable;

public class ExceptionWrapper extends RuntimeException {
    private Serializable context;

    public ExceptionWrapper(Serializable context, Throwable cause) {
        super(cause);
        this.context = context;
    }

    public Serializable getContext() {
        return context;
    }
}
