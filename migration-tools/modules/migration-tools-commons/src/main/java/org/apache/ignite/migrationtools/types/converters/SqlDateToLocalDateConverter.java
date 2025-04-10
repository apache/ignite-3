/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.types.converters;

import java.sql.Date;
import java.time.LocalDate;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts SQL Date to LocalDateTime. */
public class SqlDateToLocalDateConverter implements TypeConverter<Date, LocalDate> {
    @Override
    public LocalDate toColumnType(Date date) throws Exception {
        return date.toLocalDate();
    }

    @Override
    public Date toObjectType(LocalDate date) throws Exception {
        return Date.valueOf(date);
    }
}
