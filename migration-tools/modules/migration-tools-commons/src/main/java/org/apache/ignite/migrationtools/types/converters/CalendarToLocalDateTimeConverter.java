/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.types.converters;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts Java Calendars to LocalDateTime. */
public class CalendarToLocalDateTimeConverter implements TypeConverter<Calendar, LocalDateTime> {
    @Override
    public LocalDateTime toColumnType(Calendar calendar) throws Exception {
        return LocalDateTime.ofInstant(calendar.toInstant(), ZoneOffset.UTC);
    }

    @Override
    public Calendar toObjectType(LocalDateTime dateTime) throws Exception {
        return GregorianCalendar.from(ZonedDateTime.of(dateTime, ZoneOffset.UTC));
    }
}
