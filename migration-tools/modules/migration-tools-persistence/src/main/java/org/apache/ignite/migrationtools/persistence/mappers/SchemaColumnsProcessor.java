/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.persistence.mappers;

import java.util.Map;
import java.util.concurrent.Flow;
import org.apache.ignite3.table.DataStreamerItem;
import org.apache.ignite3.table.Tuple;

/** SchemaColumnsProcessor. */
public interface SchemaColumnsProcessor extends Flow.Processor<Map.Entry<Object, Object>, DataStreamerItem<Map.Entry<Tuple, Tuple>>> {
    SchemaColumnProcessorStats getStats();
}
