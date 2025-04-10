/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.types.converters;

import java.time.Instant;
import java.util.Date;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts Java Date Calendars to Instants. */
public class DateToInstantConverter implements TypeConverter<Date, Instant> {
    @Override
    public Instant toColumnType(Date instant) throws Exception {
        return instant.toInstant();
    }

    @Override
    public Date toObjectType(Instant instant) throws Exception {
        return Date.from(instant);
    }
}
