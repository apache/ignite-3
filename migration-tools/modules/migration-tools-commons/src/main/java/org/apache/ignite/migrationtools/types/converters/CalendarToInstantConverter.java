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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts Java Calendars to Instants. */
public class CalendarToInstantConverter implements TypeConverter<Calendar, Instant> {
    @Override
    public Instant toColumnType(Calendar calendar) throws Exception {
        return calendar.toInstant();
    }

    @Override
    public Calendar toObjectType(Instant instant) throws Exception {
        return GregorianCalendar.from(ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()));
    }
}
