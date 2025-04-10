/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.types.converters;

import java.sql.Timestamp;
import java.time.Instant;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts SQL Timestamp to Instant. */
public class SqlTimestampToInstantConverter implements TypeConverter<Timestamp, Instant> {
    @Override
    public Instant toColumnType(Timestamp timestamp) throws Exception {
        return timestamp.toInstant();
    }

    @Override
    public Timestamp toObjectType(Instant time) throws Exception {
        return Timestamp.from(time);
    }
}
