package org.apache.ignite.internal.calcite.util;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;
import org.apache.ignite.internal.util.Cursor;

public class Commons {
    public static List<List<?>> getAllFromCursor(Cursor<List<?>> cur) {
        return Streams.stream((Iterable<List<?>>)cur).collect(Collectors.toList());
    }
}
