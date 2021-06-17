package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.jetbrains.annotations.NotNull;

public class FilteringIterator<T> implements Iterator<T> {
    private final Iterator<T> delegate;

    private final Predicate<T> pred;

    private T cur;

    public FilteringIterator(
        @NotNull Iterator<T> delegate,
        @NotNull Predicate<T> pred
    ) {
        this.delegate = delegate;
        this.pred = pred;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        advance();

        return cur != null;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        advance();

        if (cur == null)
            throw new NoSuchElementException();

        T tmp = cur;

        cur = null;

        return tmp;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        delegate.remove();
    }

    /** */
    private void advance() {
        if (cur != null)
            return;

        while (delegate.hasNext() && cur == null) {
            cur = delegate.next();

            if (!pred.test(cur))
                cur = null;
        }
    }
}
