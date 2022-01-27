package org.apache.ignite.internal.metastorage.server;

import java.util.Arrays;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractBinaryCondition extends AbstractCondition {
    
    private final Condition leftCondition;
    
    private final Condition rightCondition;
    
    private final byte[][] keys;
    
    private final int arity;
    
    public AbstractBinaryCondition(Condition leftCondition,
            Condition rightCondition,
            ConditionBranch andThen,
            ConditionBranch orElse) {
        super(andThen, orElse);
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
        keys = ArrayUtils.concat(leftCondition.keys(), rightCondition.keys());
        arity = leftCondition.arity() + rightCondition.arity();
    }
    
    @Override
    public @NotNull byte[][] keys() {
        return keys;
    }
    
    @Override
    public boolean test(Entry... e) {
        return combine(leftCondition.test(Arrays.copyOf(e, leftCondition.arity())),
                rightCondition.test(Arrays.copyOfRange(e, leftCondition.arity() + 1, rightCondition.arity())));
    }
    
    @Override
    public int arity() {
        return arity;
    }
    
    public abstract boolean combine(boolean left, boolean right);
    
}
