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
import java.util.Date;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts Java Date to LocalDateTime. */
public class DateToLocalDateTimeConverter implements TypeConverter<Date, LocalDateTime> {
    @Override
    public LocalDateTime toColumnType(Date date) throws Exception {
        return LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC);
    }

    @Override
    public Date toObjectType(LocalDateTime dateTime) throws Exception {
        return Date.from(dateTime.toInstant(ZoneOffset.UTC));
    }
}
