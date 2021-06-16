package org.apache.ignite.lang;

import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.Test;

public class IgniteLoggerTest extends IgniteAbstractTest {

    @Test
    public void test() {
        log.info("This is a log string with parameter {}", 10);
    }
}
