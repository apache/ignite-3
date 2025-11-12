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

package org.apache.ignite.internal.thread;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * This class contains utility methods for working with threads.
 */
public class ThreadUtils {
    /** Thread dump message. */
    public static final String THREAD_DUMP_MSG = "Thread dump at ";

    /** Date format for thread dumps. */
    private static final DateTimeFormatter THREAD_DUMP_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneId.systemDefault());

    /**
     * Short date format pattern for log messages in "quiet" mode. Only time is included since we don't expect "quiet" mode to be used for
     * longer runs.
     */
    private static final DateTimeFormatter SHORT_DATE_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    /** System line separator. */
    private static final String NL = System.lineSeparator();

    /**
     * Performs thread dump and prints all available info to the given log
     * with WARN or ERROR logging level depending on {@code isErrorLevel} parameter.
     *
     * @param log Logger.
     * @param message Additional message to print with the thread dump.
     * @param isErrorLevel {@code true} if thread dump must be printed with ERROR logging level,
     *      {@code false} if thread dump must be printed with WARN logging level.
     */
    public static void dumpThreads(IgniteLogger log, @Nullable String message, boolean isErrorLevel) {
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();

        Set<Long> deadlockedThreadsIds = getDeadlockedThreadIds(mxBean);

        if (deadlockedThreadsIds.isEmpty()) {
            logMessage(log, "No deadlocked threads detected.", isErrorLevel);
        } else {
            logMessage(log, "Deadlocked threads detected (see thread dump below) "
                    + "[deadlockedThreadsCount=" + deadlockedThreadsIds.size() + ']', isErrorLevel);
        }

        ThreadInfo[] threadInfos =
                mxBean.dumpAllThreads(mxBean.isObjectMonitorUsageSupported(), mxBean.isSynchronizerUsageSupported());

        StringBuilder sb = new StringBuilder(THREAD_DUMP_MSG)
                .append(message == null ? "" : message)
                .append(THREAD_DUMP_FMT.format(Instant.ofEpochMilli(System.currentTimeMillis())))
                .append(NL);

        for (ThreadInfo info : threadInfos) {
            if (info == null) {
                continue;
            }

            printThreadInfo(info, sb, deadlockedThreadsIds);

            sb.append(NL);

            if (info.getLockedSynchronizers() != null && info.getLockedSynchronizers().length > 0) {
                printSynchronizersInfo(info.getLockedSynchronizers(), sb);

                sb.append(NL);
            }
        }

        sb.append(NL);

        logMessage(log, sb.toString(), isErrorLevel);
    }

    /**
     * Prints message to the given log with WARN or ERROR logging level depending on {@code isErrorLevel} parameter.
     *
     * @param log Logger.
     * @param message Message.
     * @param isErrorLevel {@code true} if message must be printed with ERROR logging level,
     *      {@code false} if message must be printed with WARN logging level.
     */
    private static void logMessage(IgniteLogger log, String message, boolean isErrorLevel) {
        if (isErrorLevel) {
            log.error(message);
        } else {
            log.warn(message);
        }
    }

    /**
     * Get deadlocks from the thread bean.
     *
     * @param mxBean The management interface for the thread system.
     * @return the set of deadlocked threads (may be empty Set, but never {@code null}).
     */
    private static Set<Long> getDeadlockedThreadIds(ThreadMXBean mxBean) {
        long[] deadlockedIds = mxBean.isSynchronizerUsageSupported()
                ? mxBean.findDeadlockedThreads() : null;

        Set<Long> deadlockedThreadsIds;

        if (deadlockedIds != null && deadlockedIds.length != 0) {
            Set<Long> set = new HashSet<>();

            for (long id : deadlockedIds) {
                set.add(id);
            }

            deadlockedThreadsIds = Collections.unmodifiableSet(set);
        } else {
            deadlockedThreadsIds = Collections.emptySet();
        }

        return deadlockedThreadsIds;
    }

    /**
     * Prints single thread info to a buffer.
     *
     * @param threadInfo Thread info.
     * @param stringBuilder Buffer.
     */
    private static void printThreadInfo(ThreadInfo threadInfo, StringBuilder stringBuilder, Set<Long> deadlockedIdSet) {
        long id = threadInfo.getThreadId();

        if (deadlockedIdSet.contains(id)) {
            stringBuilder.append("##### DEADLOCKED ");
        }

        stringBuilder.append("Thread [name=\"").append(threadInfo.getThreadName())
                .append("\", id=").append(threadInfo.getThreadId())
                .append(", state=").append(threadInfo.getThreadState())
                .append(", blockCnt=").append(threadInfo.getBlockedCount())
                .append(", waitCnt=").append(threadInfo.getWaitedCount())
                .append(']').append(NL);

        LockInfo lockInfo = threadInfo.getLockInfo();

        if (lockInfo != null) {
            stringBuilder.append("    Lock [object=")
                    .append(lockInfo)
                    .append(", ownerName=")
                    .append(threadInfo.getLockOwnerName())
                    .append(", ownerId=")
                    .append(threadInfo.getLockOwnerId())
                    .append(']')
                    .append(NL);
        }

        MonitorInfo[] monitors = threadInfo.getLockedMonitors();
        StackTraceElement[] elements = threadInfo.getStackTrace();

        for (int i = 0; i < elements.length; i++) {
            StackTraceElement e = elements[i];

            stringBuilder.append("        at ").append(e.toString());

            for (MonitorInfo monitor : monitors) {
                if (monitor.getLockedStackDepth() == i) {
                    stringBuilder.append(NL).append("        - locked ").append(monitor);
                }
            }

            stringBuilder.append(NL);
        }
    }

    /**
     * Prints Synchronizers info to a buffer.
     *
     * @param syncs Synchronizers info.
     * @param stringBuilder Buffer.
     */
    private static void printSynchronizersInfo(LockInfo[] syncs, StringBuilder stringBuilder) {
        stringBuilder.append("    Locked synchronizers:");

        for (LockInfo info : syncs) {
            stringBuilder.append(NL).append("        ").append(info);
        }
    }

    /**
     * Prints stack trace of the current thread to provided logger.
     *
     * @param log Logger.
     * @param message Message to print with the stack.
     * @deprecated Calls to this method should never be committed to master.
     */
    @Deprecated
    public static void dumpStack(IgniteLogger log, String message, Object... params) {
        String reason = "Dumping stack";

        var err = new Exception(IgniteStringFormatter.format(message, params));

        if (log != null) {
            log.warn(reason, err);
        } else {
            System.err.println("[" + LocalDateTime.now().format(SHORT_DATE_FMT) + "] (err) " + reason);

            err.printStackTrace(System.err);
        }
    }
}
