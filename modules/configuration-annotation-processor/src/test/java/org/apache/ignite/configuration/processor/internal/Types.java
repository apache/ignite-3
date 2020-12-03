package org.apache.ignite.configuration.processor.internal;

public class Types {
    private static final String PKG_JAVA_LANG = "java.lang.";

    public static final String INT = PKG_JAVA_LANG + "Integer";

    public static final String LONG = PKG_JAVA_LANG + "Long";

    public static final String STRING = PKG_JAVA_LANG + "String";

    public static final String DOUBLE = PKG_JAVA_LANG + "Double";

    public static String typeName(String packageName, String className) {
        return String.format("%s.%s", packageName, className);
    }

}
