package org.apache.ignite.internal.metrics.examples.threadpool;

import org.apache.ignite.internal.metrics.AbstractMetricsSource;
import org.apache.ignite.internal.metrics.MetricsSetBuilder;

import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ThreadPoolMetricsSource extends AbstractMetricsSource<ThreadPoolMetricsSource.Holder> {
    private final ThreadPoolExecutor exec;

    public ThreadPoolMetricsSource(String name, ThreadPoolExecutor exec) {
        super(name);

        this.exec = exec;
    }


    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    @Override
    protected void init(MetricsSetBuilder bldr, Holder holder) {
        bldr.intGauge(
                "ActiveCount",
                "Approximate number of threads that are actively executing tasks.",
                exec::getActiveCount
        );

        bldr.longGauge(
                "CompletedTaskCount",
                "Approximate total number of tasks that have completed execution.",
                exec::getCompletedTaskCount
        );

        bldr.intGauge("CorePoolSize", "The core number of threads.", exec::getCorePoolSize);

        bldr.intGauge(
                "LargestPoolSize",
                "Largest number of threads that have ever simultaneously been in the pool.",
                exec::getLargestPoolSize
        );

        bldr.intGauge(
                "MaximumPoolSize",
                "The maximum allowed number of threads.",
                exec::getMaximumPoolSize
        );

        bldr.intGauge("PoolSize", "Current number of threads in the pool.", exec::getPoolSize);

        bldr.longGauge(
                "TaskCount",
                "Approximate total number of tasks that have been scheduled for execution.",
                exec::getTaskCount
        );

        bldr.intGauge("QueueSize", "Current size of the execution queue.", () -> exec.getQueue().size());

        bldr.longGauge(
                "KeepAliveTime",
                "Thread keep-alive time, which is the amount of time which threads in excess of " +
                "the core pool size may remain idle before being terminated.",
                () -> exec.getKeepAliveTime(MILLISECONDS)
        );
    }

    protected static class Holder implements AbstractMetricsSource.Holder<Holder> {

    }
}
