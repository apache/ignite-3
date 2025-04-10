/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.types.converters;

import java.sql.Time;
import java.time.LocalTime;
import org.apache.ignite3.table.mapper.TypeConverter;

/** Converts SQL Time to Local Time. */
public class SqlTimeToLocalTimeConverter implements TypeConverter<Time, LocalTime> {
    @Override
    public LocalTime toColumnType(Time time) throws Exception {
        return time.toLocalTime();
    }

    @Override
    public Time toObjectType(LocalTime localTime) throws Exception {
        return Time.valueOf(localTime);
    }
}
