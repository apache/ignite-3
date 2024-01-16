package org.apache.ignite.compute;

public class JobExecutionOptions {

    /**
     * Default job execution options with priority default value = 0 and max retries default value = 0.
     */
    public static final JobExecutionOptions DEFAULT = builder().priority(0).maxRetries(0).build();

    private final int priority;
    private final int maxRetries;

    /**
     * Constructor.
     *
     * @param priority Job execution priority.
     * @param maxRetries Number of times to retry job execution in case of failure, 0 to not retry.
     */
    private JobExecutionOptions(int priority, int maxRetries) {
        this.priority = priority;
        this.maxRetries = maxRetries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int priority() {
        return priority;
    }

    public int maxRetries() {
        return maxRetries;
    }

    /** JobExecutionOptions builder. */
    public static class Builder {
        private int priority;

        private int maxRetries;

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public JobExecutionOptions build() {
            return new JobExecutionOptions(priority, maxRetries);
        }
    }
}
