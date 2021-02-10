package org.apache.ignite.raft;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO asch Currently used only for parsing peer ids.
 */
public class StringUtils {
    public static final String IPV6_START_MARK     = "[";

    public static final String IPV6_END_MARK       = "]";

    private static final int   IPV6_ADDRESS_LENGTH = 16;

    public static final String EMPTY = "";
    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    public StringUtils() {
    }

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }

    public static boolean isBlank(CharSequence cs) {
        int strLen;
        if (cs != null && (strLen = cs.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }

    public static String[] split(String str, char separatorChar) {
        return splitWorker(str, separatorChar, false);
    }

    private static String[] splitWorker(String str, char separatorChar, boolean preserveAllTokens) {
        if (str == null) {
            return null;
        } else {
            int len = str.length();
            if (len == 0) {
                return EMPTY_STRING_ARRAY;
            } else {
                List<String> list = new ArrayList();
                int i = 0;
                int start = 0;
                boolean match = false;
                boolean lastMatch = false;

                while(true) {
                    while(i < len) {
                        if (str.charAt(i) == separatorChar) {
                            if (match || preserveAllTokens) {
                                list.add(str.substring(start, i));
                                match = false;
                                lastMatch = true;
                            }

                            ++i;
                            start = i;
                        } else {
                            lastMatch = false;
                            match = true;
                            ++i;
                        }
                    }

                    if (match || preserveAllTokens && lastMatch) {
                        list.add(str.substring(start, i));
                    }

                    return (String[])list.toArray(new String[list.size()]);
                }
            }
        }
    }

    public static boolean isNumeric(String str) {
        if (str == null) {
            return false;
        } else {
            int sz = str.length();

            for(int i = 0; i < sz; ++i) {
                if (!Character.isDigit(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        }
    }

    public static boolean equals(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equals(str2);
    }

    /**
     * <p>Splits the provided text into an array with a maximum length,
     * separators specified, preserving all tokens, including empty tokens
     * created by adjacent separators.</p>
     *
     * <p>The separator is not included in the returned String array.
     * Adjacent separators are treated as separators for empty tokens.
     * Adjacent separators are treated as one separator.</p>
     *
     * <p>A <code>null</code> input String returns <code>null</code>.
     * A <code>null</code> separatorChars splits on whitespace.</p>
     *
     * <p>If more than <code>max</code> delimited substrings are found, the last
     * returned string includes all characters after the first <code>max - 1</code>
     * returned strings (including separator characters).</p>
     *
     * <pre>
     * StringUtils.splitPreserveAllTokens(null, *, *)            = null
     * StringUtils.splitPreserveAllTokens("", *, *)              = []
     * StringUtils.splitPreserveAllTokens("ab de fg", null, 0)   = ["ab", "cd", "ef"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 0) = ["ab", "cd", "ef"]
     * StringUtils.splitPreserveAllTokens("ab:cd:ef", ":", 0)    = ["ab", "cd", "ef"]
     * StringUtils.splitPreserveAllTokens("ab:cd:ef", ":", 2)    = ["ab", "cd:ef"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 2) = ["ab", "  de fg"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 3) = ["ab", "", " de fg"]
     * StringUtils.splitPreserveAllTokens("ab   de fg", null, 4) = ["ab", "", "", "de fg"]
     * </pre>
     *
     * @param str  the String to parse, may be <code>null</code>
     * @param separatorChars  the characters used as the delimiters,
     *  <code>null</code> splits on whitespace
     * @param max  the maximum number of elements to include in the
     *  array. A zero or negative value implies no limit
     * @return an array of parsed Strings, <code>null</code> if null String input
     * @since 2.1
     */
    public static String[] splitPreserveAllTokens(String str, String separatorChars, int max) {
        return splitWorker(str, separatorChars, max, true);
    }

    /**
     * Performs the logic for the <code>split</code> and
     * <code>splitPreserveAllTokens</code> methods that return a maximum array
     * length.
     *
     * @param str  the String to parse, may be <code>null</code>
     * @param separatorChars the separate character
     * @param max  the maximum number of elements to include in the
     *  array. A zero or negative value implies no limit.
     * @param preserveAllTokens if <code>true</code>, adjacent separators are
     * treated as empty token separators; if <code>false</code>, adjacent
     * separators are treated as one separator.
     * @return an array of parsed Strings, <code>null</code> if null String input
     */
    private static String[] splitWorker(String str, String separatorChars, int max, boolean preserveAllTokens) {
        // Performance tuned for 2.0 (JDK1.4)
        // Direct code is quicker than StringTokenizer.
        // Also, StringTokenizer uses isSpace() not isWhitespace()

        if (str == null) {
            return null;
        }
        int len = str.length();
        if (len == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List list = new ArrayList();
        int sizePlus1 = 1;
        int i = 0, start = 0;
        boolean match = false;
        boolean lastMatch = false;
        if (separatorChars == null) {
            // Null separator means use whitespace
            while (i < len) {
                if (Character.isWhitespace(str.charAt(i))) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {
            // Optimise 1 character case
            char sep = separatorChars.charAt(0);
            while (i < len) {
                if (str.charAt(i) == sep) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else {
            // standard case
            while (i < len) {
                if (separatorChars.indexOf(str.charAt(i)) >= 0) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        }
        if (match || (preserveAllTokens && lastMatch)) {
            list.add(str.substring(start, i));
        }
        return (String[]) list.toArray(new String[list.size()]);
    }

    /**
     * <p>Checks if String contains a search String irrespective of case,
     * handling <code>null</code>. Case-insensitivity is defined as by
     * {@link String#equalsIgnoreCase(String)}.
     *
     * <p>A <code>null</code> String will return <code>false</code>.</p>
     *
     * <pre>
     * StringUtils.contains(null, *) = false
     * StringUtils.contains(*, null) = false
     * StringUtils.contains("", "") = true
     * StringUtils.contains("abc", "") = true
     * StringUtils.contains("abc", "a") = true
     * StringUtils.contains("abc", "z") = false
     * StringUtils.contains("abc", "A") = true
     * StringUtils.contains("abc", "Z") = false
     * </pre>
     *
     * @param str  the String to check, may be null
     * @param searchStr  the String to find, may be null
     * @return true if the String contains the search String irrespective of
     * case or false if not or <code>null</code> string input
     */
    public static boolean containsIgnoreCase(String str, String searchStr) {
        if (str == null || searchStr == null) {
            return false;
        }
        int len = searchStr.length();
        int max = str.length() - len;
        for (int i = 0; i <= max; i++) {
            if (str.regionMatches(true, i, searchStr, 0, len)) {
                return true;
            }
        }
        return false;
    }

    // -----------------------------------------------------------------------
    /**
     * <p>Splits the provided text into an array, using whitespace as the
     * separator, preserving all tokens, including empty tokens created by
     * adjacent separators. This is an alternative to using StringTokenizer.
     * Whitespace is defined by {@link Character#isWhitespace(char)}.</p>
     *
     * <p>The separator is not included in the returned String array.
     * Adjacent separators are treated as separators for empty tokens.
     * For more control over the split use the StrTokenizer class.</p>
     *
     * <p>A <code>null</code> input String returns <code>null</code>.</p>
     *
     * <pre>
     * StringUtils.splitPreserveAllTokens(null)       = null
     * StringUtils.splitPreserveAllTokens("")         = []
     * StringUtils.splitPreserveAllTokens("abc def")  = ["abc", "def"]
     * StringUtils.splitPreserveAllTokens("abc  def") = ["abc", "", "def"]
     * StringUtils.splitPreserveAllTokens(" abc ")    = ["", "abc", ""]
     * </pre>
     *
     * @param str  the String to parse, may be <code>null</code>
     * @return an array of parsed Strings, <code>null</code> if null String input
     * @since 2.1
     */
    public static String[] splitPreserveAllTokens(String str) {
        return splitWorker(str, null, -1, true);
    }

    /**
     * <p>Splits the provided text into an array, separator specified,
     * preserving all tokens, including empty tokens created by adjacent
     * separators. This is an alternative to using StringTokenizer.</p>
     *
     * <p>The separator is not included in the returned String array.
     * Adjacent separators are treated as separators for empty tokens.
     * For more control over the split use the StrTokenizer class.</p>
     *
     * <p>A <code>null</code> input String returns <code>null</code>.</p>
     *
     * <pre>
     * StringUtils.splitPreserveAllTokens(null, *)         = null
     * StringUtils.splitPreserveAllTokens("", *)           = []
     * StringUtils.splitPreserveAllTokens("a.b.c", '.')    = ["a", "b", "c"]
     * StringUtils.splitPreserveAllTokens("a..b.c", '.')   = ["a", "", "b", "c"]
     * StringUtils.splitPreserveAllTokens("a:b:c", '.')    = ["a:b:c"]
     * StringUtils.splitPreserveAllTokens("a\tb\nc", null) = ["a", "b", "c"]
     * StringUtils.splitPreserveAllTokens("a b c", ' ')    = ["a", "b", "c"]
     * StringUtils.splitPreserveAllTokens("a b c ", ' ')   = ["a", "b", "c", ""]
     * StringUtils.splitPreserveAllTokens("a b c  ", ' ')   = ["a", "b", "c", "", ""]
     * StringUtils.splitPreserveAllTokens(" a b c", ' ')   = ["", a", "b", "c"]
     * StringUtils.splitPreserveAllTokens("  a b c", ' ')  = ["", "", a", "b", "c"]
     * StringUtils.splitPreserveAllTokens(" a b c ", ' ')  = ["", a", "b", "c", ""]
     * </pre>
     *
     * @param str  the String to parse, may be <code>null</code>
     * @param separatorChar  the character used as the delimiter,
     *  <code>null</code> splits on whitespace
     * @return an array of parsed Strings, <code>null</code> if null String input
     * @since 2.1
     */
    public static String[] splitPreserveAllTokens(String str, char separatorChar) {
        return splitWorker(str, separatorChar, true);
    }

    /**
     * Parse peerId from string that generated by {@link #toString()}
     * This method can support parameter string values are below:
     *
     * <pre>
     * PeerId.parse("a:b")          = new PeerId("a", "b", 0 , -1)
     * PeerId.parse("a:b:c")        = new PeerId("a", "b", "c", -1)
     * PeerId.parse("a:b::d")       = new PeerId("a", "b", 0, "d")
     * PeerId.parse("a:b:c:d")      = new PeerId("a", "b", "c", "d")
     * </pre>
     *
     */
    public static String[] parsePeerId(String s) {
        if (s.startsWith(IPV6_START_MARK) && StringUtils.containsIgnoreCase(s, IPV6_END_MARK)) {
            String ipv6Addr;
            if (s.endsWith(IPV6_END_MARK)) {
                ipv6Addr = s;
            } else {
                ipv6Addr = s.substring(0, (s.indexOf(IPV6_END_MARK) + 1));
            }
            if (!isIPv6(ipv6Addr)) {
                throw new IllegalArgumentException("The IPv6 address(\"" + ipv6Addr + "\") is incorrect.");
            }
            String tempString = s.substring((s.indexOf(ipv6Addr) + ipv6Addr.length()));
            if (tempString.startsWith(":")) {
                tempString = tempString.substring(1);
            }
            String[] tempArr = StringUtils.splitPreserveAllTokens(tempString, ':');
            String[] result = new String[1 + tempArr.length];
            result[0] = ipv6Addr;
            System.arraycopy(tempArr, 0, result, 1, tempArr.length);
            return result;
        } else {
            return StringUtils.splitPreserveAllTokens(s, ':');
        }
    }

    /**
     * check whether the ip address is IPv6.
     *
     * @param addr ip address
     * @return boolean
     */
    private static boolean isIPv6(String addr) {
        try {
            return InetAddress.getByName(addr).getAddress().length == IPV6_ADDRESS_LENGTH;
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
