package org.apache.ignite.internal.util;

/**
 * Pair of objects.
 *
 * @param <T> First object.
 * @param <V> Second object.
 */
public class Pair<T, V> {
    /** First obj. */
    private final T first;

    /** Second obj. */
    private final V second;

    /**
     * Constructor.
     *
     * @param first  First object.
     * @param second Second object.
     */
    public Pair(T first, V second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Get the first object.
     */
    public T getFirst() {
        return first;
    }

    /**
     * Get the second object.
     */
    public V getSecond() {
        return second;
    }
}
