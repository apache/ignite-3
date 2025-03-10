package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;

import org.apache.ignite.internal.testframework.WithSystemProperty;

// TODO Remove https://issues.apache.org/jira/browse/IGNITE-22522
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
public class ItColocationDurableFinishTest extends ItDurableFinishTest {
}
