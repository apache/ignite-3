/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
