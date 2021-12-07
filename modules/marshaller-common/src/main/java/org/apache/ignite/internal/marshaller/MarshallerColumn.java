package org.apache.ignite.internal.marshaller;

import java.util.function.Supplier;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.jetbrains.annotations.Nullable;

public class MarshallerColumn {
    /** Default "default value supplier". */
    private static final Supplier<Object> NULL_SUPPLIER = () -> null;

    /**
     * Column name.
     */
    private final String name;

    /**
     * Column type.
     */
    private final BinaryMode type;

    /**
     * Default value supplier.
     */
    @IgniteToStringExclude
    private final Supplier<Object> defValSup;

    /**
     * Constructor.
     *
     * @param name      Column name.
     * @param type      An instance of column data type.
     */
    public MarshallerColumn(String name, BinaryMode type) {
        this(name, type, null);
    }

    /**
     * Constructor.
     *
     * @param name      Column name.
     * @param type      An instance of column data type.
     * @param defValSup Default value supplier.
     */
    public MarshallerColumn(String name, BinaryMode type, @Nullable Supplier<Object> defValSup) {
        this.name = name;
        this.type = type;
        this.defValSup = defValSup == null ? NULL_SUPPLIER : defValSup;
    }

    public String name() {
        return name;
    }

    public BinaryMode type() {
        return type;
    }

    public Object defaultValue() {
        return defValSup.get();
    }
}
