package org.apache.ignite.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Test retry policy.
  */
public class TestRetryPolicy extends RetryLimitPolicy {
    public final List<RetryPolicyContext> invocations = new ArrayList<>();

    @Override
    public boolean shouldRetry(RetryPolicyContext context) {
        invocations.add(context);

        return super.shouldRetry(context);
    }
}
