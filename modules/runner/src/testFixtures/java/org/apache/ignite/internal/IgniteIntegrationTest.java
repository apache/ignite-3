/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.internal.junit.StopAllIgnitesAfterTests;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * A test that starts some Ignite instances (and cleans them up later if they are forgotten).
 */
// The order is important here.
@ExtendWith({WorkDirectoryExtension.class, StopAllIgnitesAfterTests.class})
public abstract class IgniteIntegrationTest extends BaseIgniteAbstractTest {
    @BeforeAll
    public static void assertParanoidLeakDetectionProperty() {
        assertThat(System.getProperty("io.netty.leakDetectionLevel"), is("paranoid"));
    }
}
