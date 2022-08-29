package org.apache.ignite.internal.binarytuple;

// TODO: Better name?
public interface BinaryTupleContainer {
    /**
     * Gets the underlying binary tuple.
     *
     * @return Underlying tuple or null when not applicable.
     */
    BinaryTupleReader binaryTuple();
}
