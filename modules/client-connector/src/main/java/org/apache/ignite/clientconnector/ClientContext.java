package org.apache.ignite.clientconnector;

import java.util.BitSet;

class ClientContext {
    private final int verMajor;

    private final int verMinor;

    private final int verPatch;

    private final int clientCode;

    private final BitSet features;

    public ClientContext(int verMajor, int verMinor, int verPatch, int clientCode, BitSet features) {
        this.verMajor = verMajor;
        this.verMinor = verMinor;
        this.verPatch = verPatch;
        this.clientCode = clientCode;
        this.features = features;
    }

    @Override
    public String toString() {
        return "ClientContext{" +
                "verMajor=" + verMajor +
                ", verMinor=" + verMinor +
                ", verPatch=" + verPatch +
                ", clientCode=" + clientCode +
                ", features=" + features +
                '}';
    }
}
