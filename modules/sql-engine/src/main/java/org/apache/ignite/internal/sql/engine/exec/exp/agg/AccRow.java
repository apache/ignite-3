package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import java.util.List;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

public class AccRow<RowT> {

    private final Object[] data;

    public final List<AccumulatorWrapper<RowT>> wrappers;

    private final AggregateType type;

    private AccState accState;

    private final int writeOffset;

    private final int readOffset;

    public int size() {
        return data.length;
    }

    public AccRow(List<AccumulatorWrapper<RowT>> wrappers, IgniteTypeFactory typeFactory, AggregateType type,
            int writeOffset, int readOffset) {

        int rowSize = 0;

        for (AccumulatorWrapper<RowT> wrapper : wrappers) {
            rowSize += wrapper.accumulator().state(typeFactory).size();
        }

        this.type = type;
        this.data = new Object[rowSize];
        this.wrappers = wrappers;

        this.writeOffset = writeOffset;
        this.readOffset = readOffset;

        accState = new AccState(data);
        accState.offset = readOffset;
    }

    public void update(RowT row) {
        accState.reset();

        for (AccumulatorWrapper<RowT> wrapper : wrappers) {
            wrapper.update(accState, row);
        }
    }

    public void appendTo(Object[] output) {
        accState.reset();

        if (type == AggregateType.REDUCE || type == AggregateType.SINGLE) {
            for (int i = 0; i < wrappers.size(); i++) {
                AccumulatorWrapper<RowT> wrapper = wrappers.get(i);
                output[i + writeOffset] = wrapper.end();
            }
        } else {
            for (AccumulatorWrapper<RowT> wrapper : wrappers) {
                wrapper.writeTo(accState);
            }

            for (int i = 0; i < accState.state.length; i++) {
                output[i + readOffset] = accState.state[i];
            }
        }
    }
}
