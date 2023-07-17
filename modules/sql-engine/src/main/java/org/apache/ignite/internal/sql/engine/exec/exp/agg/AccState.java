package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.jetbrains.annotations.Nullable;

public class AccState {

    public final Object[] state;

    public int position;

    public int offset;

    private int maxPosition;

    private final ArrayDeque<Accumulator> stack = new ArrayDeque<>();

    public AccState(Object[] state) {
        this.state = state;
    }

    public void reset() {
        position = 0;
        maxPosition = 0;
    }

    public void next(Accumulator accumulator) {
        List<RelDataType> args = accumulator.state(Commons.typeFactory());

        position = maxPosition;
        maxPosition += args.size();

//        stack.push(accumulator);
    }

    public void pop(Accumulator accumulator) {
//        if (Objects.equals(stack.peek(), accumulator)) {
//            List<RelDataType> args = accumulator.state(Commons.typeFactory());
//
//            position -= args.size();
//            maxPosition -= args.size();
//
//            stack.pop();
//        }
    }

    public void set(int idx, @Nullable Object value) {
        int p = idx + position;
        assert p >= 0 && p < maxPosition : "Invalid position: " + idx + " . Min: " + position + " Max: " + maxPosition;

        state[p] = value;
    }

    public void update(int idx, @Nullable Object val) {
        state[idx] = val;
    }

    public Object get(int idx) {
        return state[idx + position];
    }

    public Object get(int idx, Object defaultValue) {
        return state[idx + position];
    }
}
