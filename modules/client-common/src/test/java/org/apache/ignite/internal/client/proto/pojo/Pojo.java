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

package org.apache.ignite.internal.client.proto.pojo;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * POJO with all supported types.
 */
public class Pojo {
    private boolean bool;
    private byte byteField;
    private short shortField;
    private int intField;
    private Long longField;
    private float floatField;
    private double doubleField;
    private String string;
    private UUID uuid;
    private byte[] byteArray;
    private LocalTime localTime;
    private LocalDate localDate;
    private LocalDateTime localDateTime;
    private Instant instant;
    private Period period;
    private Duration duration;
    private Pojo childPojo;

    public Pojo() {
    }

    /** Constructor. */
    public Pojo(
            boolean nested,
            boolean bool, byte b, short shortField, int intField, long longField, float floatField, double doubleField, String string,
            UUID uuid, byte[] byteArray, LocalTime localTime, LocalDate localDate, LocalDateTime localDateTime,
            Instant instant, Period period, Duration duration
    ) {
        this.bool = bool;
        this.byteField = b;
        this.shortField = shortField;
        this.intField = intField;
        this.longField = longField;
        this.floatField = floatField;
        this.doubleField = doubleField;
        this.string = string;
        this.uuid = uuid;
        this.byteArray = byteArray;
        this.localTime = localTime;
        this.localDate = localDate;
        this.localDateTime = localDateTime;
        this.instant = instant;
        this.period = period;
        this.duration = duration;
        if (nested) {
            this.childPojo = new Pojo(false, bool, b, shortField, intField, longField, floatField, doubleField, string, uuid,
                    byteArray, localTime, localDate, localDateTime, instant, period, duration);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Pojo pojo = (Pojo) o;
        return bool == pojo.bool && byteField == pojo.byteField && shortField == pojo.shortField
                && intField == pojo.intField && Float.compare(floatField, pojo.floatField) == 0
                && Double.compare(doubleField, pojo.doubleField) == 0 && Objects.equals(longField, pojo.longField) && Objects.equals(
                string, pojo.string)
                && Objects.equals(uuid, pojo.uuid) && Arrays.equals(byteArray, pojo.byteArray)
                && Objects.equals(localTime, pojo.localTime) && Objects.equals(localDate, pojo.localDate)
                && Objects.equals(localDateTime, pojo.localDateTime) && Objects.equals(instant, pojo.instant)
                && Objects.equals(period, pojo.period) && Objects.equals(duration, pojo.duration)
                && Objects.equals(childPojo, pojo.childPojo);
    }

    @Override
    public int hashCode() {
        int result = Boolean.hashCode(bool);
        result = 31 * result + byteField;
        result = 31 * result + shortField;
        result = 31 * result + intField;
        result = 31 * result + Objects.hashCode(longField);
        result = 31 * result + Float.hashCode(floatField);
        result = 31 * result + Double.hashCode(doubleField);
        result = 31 * result + Objects.hashCode(string);
        result = 31 * result + Objects.hashCode(uuid);
        result = 31 * result + Arrays.hashCode(byteArray);
        result = 31 * result + Objects.hashCode(localTime);
        result = 31 * result + Objects.hashCode(localDate);
        result = 31 * result + Objects.hashCode(localDateTime);
        result = 31 * result + Objects.hashCode(instant);
        result = 31 * result + Objects.hashCode(period);
        result = 31 * result + Objects.hashCode(duration);
        result = 31 * result + Objects.hashCode(childPojo);
        return result;
    }

    @Override
    public String toString() {
        return "Pojo{" + "bool=" + bool + ", b=" + byteField + ", s=" + shortField + ", i=" + intField + ", l=" + longField
                + ", f=" + floatField + ", d=" + doubleField + ", str='" + string + '\'' + ", uuid=" + uuid
                + ", byteArray=" + Arrays.toString(byteArray) + ", localTime=" + localTime + ", localDate=" + localDate
                + ", localDateTime=" + localDateTime + ", instant=" + instant + ", period=" + period + ", duration=" + duration
                + ", childPojo=" + childPojo
                + '}';
    }

    /**
     * Creates instance filled with test data.
     */
    public static Pojo generateTestPojo() {
        return generateTestPojo(false);
    }

    /**
     * Creates instance filled with test data.
     *
     * @param nested {@code true} if childPojo field needs to be filled
     */
    public static Pojo generateTestPojo(boolean nested) {
        return new Pojo(nested,
                true, (byte) 4, (short) 8, 15, 16L, 23.0f, 42.0d, "TEST_STRING", UUID.randomUUID(), new byte[]{1, 2, 3},
                LocalTime.now(), LocalDate.now(), LocalDateTime.now(), Instant.now(),
                Period.of(1, 2, 3), Duration.of(1, ChronoUnit.DAYS)
        );
    }
}
