package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Iterator;
import java.util.function.Function;

public class TransformingIterator<Tin, Tout> implements Iterator<Tout> {
    private final Iterator<Tin> delegate;

    private final Function<Tin, Tout> transformation;

    public TransformingIterator(Iterator<Tin> delegate, Function<Tin, Tout> transformation) {
        this.delegate = delegate;
        this.transformation = transformation;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return delegate.hasNext();
    }

    /** {@inheritDoc} */
    @Override public Tout next() {
        return transformation.apply(delegate.next());
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        delegate.remove();
    }
}
