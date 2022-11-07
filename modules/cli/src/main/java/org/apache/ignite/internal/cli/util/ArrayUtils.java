package org.apache.ignite.internal.cli.util;

public final class ArrayUtils {

    private ArrayUtils() {
    }

    public static String findLastNotEmptyWord(String[] words) {
        for (int i = words.length - 1; i >= 0; i--) {
            if (!words[i].isEmpty()) {
                return words[i];
            }
        }
        return "";
    }
}
