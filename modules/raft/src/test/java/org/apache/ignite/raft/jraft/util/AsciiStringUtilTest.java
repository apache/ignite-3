/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class AsciiStringUtilTest {

    @Test
    public void codecTest() {
        final String asciiText = "127.0.0.1:8080";
        final byte[] bytes1 = AsciiStringUtil.unsafeEncode(asciiText);
        final byte[] bytes2 = asciiText.getBytes();
        Assert.assertArrayEquals(bytes1, bytes2);

        final String s1 = new String(bytes1);
        final String s2 = AsciiStringUtil.unsafeDecode(bytes2);
        Assert.assertEquals(s1, s2);
    }
}
