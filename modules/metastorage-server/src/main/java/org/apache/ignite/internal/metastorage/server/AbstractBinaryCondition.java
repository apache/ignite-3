package org.apache.ignite.internal.metastorage.server;

import java.util.Arrays;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractBinaryCondition implements Condition {
    
    private final Condition leftCondition;
    
    private final Condition rightCondition;
    
    private final byte[][] keys;
    
    
    public AbstractBinaryCondition(Condition leftCondition, Condition rightCondition) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
        keys = ArrayUtils.concat(leftCondition.keys(), rightCondition.keys());
    }
    
    @Override
    public @NotNull byte[][] keys() {
        return keys;
    }
    
    @Override
    public boolean test(Entry... e) {
        return combine(leftCondition.test(Arrays.copyOf(e, leftCondition.keys().length)),
                rightCondition.test(Arrays.copyOfRange(e, leftCondition.keys().length, leftCondition.keys().length + rightCondition.keys().length)));
    }
    
    protected abstract boolean combine(boolean left, boolean right);
    
    
    public Condition leftCondition() {
        return leftCondition;
    }
    
    public Condition rightCondition() {
        return rightCondition;
    }
}
