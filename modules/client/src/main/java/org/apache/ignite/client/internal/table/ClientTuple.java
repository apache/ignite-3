package org.apache.ignite.client.internal.table;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.table.Tuple;

import java.util.BitSet;
import java.util.UUID;

public class ClientTuple implements Tuple {
    @Override public <T> T valueOrDefault(String colName, T def) {
        return null;
    }

    @Override public <T> T value(String colName) {
        return null;
    }

    @Override public BinaryObject binaryObjectField(String colName) {
        return null;
    }

    @Override public byte byteValue(String colName) {
        return 0;
    }

    @Override public short shortValue(String colName) {
        return 0;
    }

    @Override public int intValue(String colName) {
        return 0;
    }

    @Override public long longValue(String colName) {
        return 0;
    }

    @Override public float floatValue(String colName) {
        return 0;
    }

    @Override public double doubleValue(String colName) {
        return 0;
    }

    @Override public String stringValue(String colName) {
        return null;
    }

    @Override public UUID uuidValue(String colName) {
        return null;
    }

    @Override public BitSet bitmaskValue(String colName) {
        return null;
    }
}
