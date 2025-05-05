/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.persistence.mappers;

/** SchemaColumnProcessorStats. */
public class SchemaColumnProcessorStats {
    private long processedElements;

    SchemaColumnProcessorStats() {
        super();
    }

    public SchemaColumnProcessorStats(long processedElements) {
        this.processedElements = processedElements;
    }

    public long getProcessedElements() {
        return processedElements;
    }

    @Override
    public String toString() {
        return "Stats{processedElements=" + processedElements + '}';
    }
}
