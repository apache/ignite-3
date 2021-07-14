package org.apache.ignite.internal.schema.row;

import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Helper class for temporal type conversions.
 * <p>
 * Provides methods to encode/decode temporal types in a compact way  for futher writing to row.
 * Conversion preserves natural type order.
 *
 * @see org.apache.ignite.internal.schema.Row
 * @see org.apache.ignite.internal.schema.RowAssembler
 */
public class TemporalTypeFormat {
    /** Month field length. */
    public static final int MONTH_FIELD_LENGTH = 4;
    /** Day field length. */
    public static final int DAY_FIELD_LENGTH = 5;
    /** Hours field length. */
    public static final int HOUR_FIELD_LENGTH = 5;
    /** Minutes field length. */
    public static final int MINUTES_FIELD_LENGTH = 6;
    /** Seconds field length. */
    public static final int SECONDS_FIELD_LENGTH = 6;
    /** Milliseconds field length. */
    public static final int MILLIS_FIELD_LENGTH = 10;

    /**
     * @param len Mask length in bits.
     * @return Mask.
     */
    private static int mask(int len) {
        return (1 << len) - 1;
    }

    /**
     * Compact LocalTime.
     *
     * @param time Time.
     * @return Encoded time.
     */
    public static int compactTime(LocalTime time) {
        int val = time.getNano() / 1_000_000; // Conver to millis.

        val |= time.getSecond() << MILLIS_FIELD_LENGTH;
        val |= time.getMinute() << (MILLIS_FIELD_LENGTH + SECONDS_FIELD_LENGTH);
        val |= time.getHour() << (MILLIS_FIELD_LENGTH + SECONDS_FIELD_LENGTH + MINUTES_FIELD_LENGTH);

        return val;
    }

    /**
     * Compact LocalDate.
     *
     * @param date Date.
     * @return Encoded date.
     */
    public static int encodeDate(LocalDate date) {
        int val = date.getDayOfMonth();

        val |= date.getMonthValue() << DAY_FIELD_LENGTH;
        val |= date.getYear() << (DAY_FIELD_LENGTH + MONTH_FIELD_LENGTH);

        return val;
    }

    /**
     * Expands to LocalTime.
     *
     * @param time Encoded time.
     * @return LocalTime instance.
     */
    public static LocalTime decodeTime(int time) {
        int millis = time & mask(MILLIS_FIELD_LENGTH);
        int sec = (time >>>= MILLIS_FIELD_LENGTH) & mask(SECONDS_FIELD_LENGTH);
        int min = (time >>>= SECONDS_FIELD_LENGTH) & mask(MINUTES_FIELD_LENGTH);
        int hour = (time >>> MINUTES_FIELD_LENGTH) & mask(HOUR_FIELD_LENGTH);

        return LocalTime.of(hour, min, sec, millis * 1_000_000 /* to nanos */);
    }

    /**
     * Expands to LocalDate.
     *
     * @param date Encoded date.
     * @return LocalDate instance.
     */
    public static LocalDate decodeDate(int date) {
        int day = (date) & mask(DAY_FIELD_LENGTH);
        int mon = (date >>= DAY_FIELD_LENGTH) & mask(MONTH_FIELD_LENGTH); // Sign matters.
        int year = (date >> MONTH_FIELD_LENGTH); // Sign matters.

        return LocalDate.of(year, mon, day);
    }
}
