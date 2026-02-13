package org.apache.ignite.internal.sql.engine.prepare;

import it.unimi.dsi.fastutil.ints.IntSet;

interface SourcesAware {
    IntSet sources();
}
