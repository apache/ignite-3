package org.apache.ignite.cli.core.call;

import java.util.Objects;

/**
 * Default implementation of {@link CallOutput} with {@link T} body.
 */
public class DefaultCallOutput<T> implements CallOutput<T> {
    private final CallOutputStatus status;
    private final T body;
    private final Throwable cause;

    private DefaultCallOutput(CallOutputStatus status, T body, Throwable cause) {
        this.status = status;
        this.body = body;
        this.cause = cause;
    }

    @Override
    public boolean hasError() {
        return cause != null;
    }

    @Override
    public boolean isEmpty() {
        return CallOutputStatus.EMPTY.equals(status);
    }

    @Override
    public Throwable errorCause() {
        return cause;
    }

    @Override
    public T body() {
        return body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCallOutput<?> that = (DefaultCallOutput<?>) o;
        return status == that.status && Objects.equals(body, that.body) && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, body, cause);
    }

    @Override
    public String toString() {
        return "DefaultCallOutput{"
            + "status="
            + status
            + ", body='"
            + body + '\''
            + ", cause="
            + cause
            + '}';
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link DefaultCallOutputBuilder}.
     */
    public static <T> DefaultCallOutputBuilder<T> builder() {
        return new DefaultCallOutputBuilder<T>();
    }

    /**
     * New successful call output with provided body.
     *
     * @param body for successful call output.
     * @return Successful call output with provided body.
     */
    public static <T> DefaultCallOutput<T> success(T body) {
        return DefaultCallOutput.<T>builder()
                .status(CallOutputStatus.SUCCESS)
                .body(body)
                .build();
    }

    /**
     * New failed call output with provided cause.
     *
     * @param cause error of failed call.
     * @return Failed call output with provided cause.
     */
    public static <T> DefaultCallOutput<T> failure(Throwable cause) {
        return DefaultCallOutput.<T>builder()
            .status(CallOutputStatus.ERROR)
            .cause(cause)
            .build();
    }

    /**
     * New empty coll output.
     *
     * @return Empty call output.
     */
    public static <T> DefaultCallOutput<T> empty() {
        return DefaultCallOutput.<T>builder()
                .status(CallOutputStatus.EMPTY)
                .build();
    }

    /**
     * Builder of {@link DefaultCallOutput}.
     */
    public static class DefaultCallOutputBuilder<T> {
        private CallOutputStatus status;
        private T body;
        private Throwable cause;

        /**
         * Builder setter.
         *
         * @param status output status.
         * @return invoked builder instance {@link DefaultCallOutputBuilder}.
         */
        public DefaultCallOutputBuilder<T> status(CallOutputStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Builder setter.
         *
         * @param body call output body.
         * @return invoked builder instance {@link DefaultCallOutputBuilder}.
         */
        public DefaultCallOutputBuilder<T> body(T body) {
            this.body = body;
            return this;
        }

        /**
         * Builder setter.
         *
         * @param cause exception cause.
         * @return invoked builder instance {@link DefaultCallOutputBuilder}.
         */
        public DefaultCallOutputBuilder<T> cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        /**
         * Build method.
         *
         * @return new {@link DefaultCallOutput} with field provided to builder.
         */
        public DefaultCallOutput<T> build() {
            return new DefaultCallOutput<>(status, body, cause);
        }
    }
}
