package org.apache.ignite.internal.table.distributed;

import java.util.Collection;
import java.util.function.BiPredicate;

public interface BatchFullPredicate<T> extends BiPredicate<Collection<T>, T> {

}
