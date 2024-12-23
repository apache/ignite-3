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

package org.apache.ignite.internal.util;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods used throughout the system.
 */
public class MyIgniteUtils {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter SHORT_DATE_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Represents timestamp as string.
     *
     * @param ts Hybrid timestamp as long.
     * @return String representation.
     */
    public static String formatDate(long ts) {
        var hybridTs = HybridTimestamp.hybridTimestamp(ts);

        return format(
                "HybridTimestamp [physical={}, logical={}]",
                dateFormat.format(new Date(hybridTs.getPhysical())),
                hybridTs.getLogical()
        );
    }

    /**
     * Prints dump for all threads.
     *
     * @param log Log.
     * @param isErrorLevel True means the dump prints at the error level.
     */
    public static void dumpThreads(@Nullable IgniteLogger log, boolean isErrorLevel) {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos =
                mxBean.dumpAllThreads(mxBean.isObjectMonitorUsageSupported(), mxBean.isSynchronizerUsageSupported());
        StringBuilder sb = new StringBuilder("Thread dump at ")
                .append(LocalDateTime.now().format(SHORT_DATE_FMT)).append('\n');
        for (ThreadInfo info : threadInfos) {
            printThreadInfo(info, sb);
            sb.append('\n');
        }
        sb.append('\n');
        if (log == null) {
            writeToFile(sb.toString());
            return;
        }
        if (isErrorLevel) {
            log.error(sb.toString());
        } else {
            log.info(sb.toString());
        }
    }

    private static void printThreadInfo(ThreadInfo threadInfo, StringBuilder sb) {
        final long id = threadInfo.getThreadId();
        sb.append("Thread [name=\"").append(threadInfo.getThreadName())
                .append("\", id=").append(threadInfo.getThreadId())
                .append(", state=").append(threadInfo.getThreadState())
                .append(", blockCnt=").append(threadInfo.getBlockedCount())
                .append(", waitCnt=").append(threadInfo.getWaitedCount()).append("]").append('\n');
        LockInfo lockInfo = threadInfo.getLockInfo();
        if (lockInfo != null) {
            sb.append("    Lock [object=").append(lockInfo)
                    .append(", ownerName=").append(threadInfo.getLockOwnerName())
                    .append(", ownerId=").append(threadInfo.getLockOwnerId()).append("]").append('\n');
        }
        MonitorInfo[] monitors = threadInfo.getLockedMonitors();
        StackTraceElement[] elements = threadInfo.getStackTrace();
        for (int i = 0; i < elements.length; i++) {
            StackTraceElement e = elements[i];
            sb.append("        at ").append(e.toString());
            for (MonitorInfo monitor : monitors) {
                if (monitor.getLockedStackDepth() == i) {
                    sb.append('\n').append("        - locked ").append(monitor);
                }
            }
            sb.append('\n');
        }
    }

    /**
     * Writes a string to file.
     *
     * @param str String to write.
     */
    private static void writeToFile(String str) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\temp\\stack_" + LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("HH_mm_ss_SSS"))));
            writer.write(str);
            writer.close();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
