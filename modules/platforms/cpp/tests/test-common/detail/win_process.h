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

#pragma once

// It's OK that this code is entirely in header as it only supposed to be included from a single file.

// clang-format off
#include <windows.h>
#include <tlhelp32.h>
// clang-format on

// FIXME: Could this be moved above windows headers as per our style guide?
#include "cmd_process.h"

#include <chrono>
#include <sstream>
#include <string>
#include <vector>

// Using NULLs as specified by WinAPI
#ifdef __JETBRAINS_IDE__
# pragma ide diagnostic ignored "modernize-use-nullptr"
#endif

namespace ignite::detail {

/**
 * Get process tree.
 * @param processId ID of the parent process.
 * @return CmdProcess tree.
 */
std::vector<DWORD> getProcessTree(DWORD processId) // NOLINT(misc-no-recursion)
{
    std::vector<DWORD> children;
    PROCESSENTRY32 pe;

    memset(&pe, 0, sizeof(PROCESSENTRY32));
    pe.dwSize = sizeof(PROCESSENTRY32);

    HANDLE hSnap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);

    if (Process32First(hSnap, &pe)) {
        BOOL bContinue = TRUE;

        while (bContinue) {
            if (pe.th32ParentProcessID == processId)
                children.push_back(pe.th32ProcessID);

            bContinue = Process32Next(hSnap, &pe);
        }
    }

    std::vector<DWORD> tree(children);
    for (auto procId : children) {
        std::vector<DWORD> childTree = getProcessTree(procId);
        tree.insert(tree.end(), childTree.begin(), childTree.end());
    }

    return tree;
}

/**
 * Implementation of CmdProcess for Windows.
 */
class WinProcess : public ignite::CmdProcess {
public:
    /**
     * Constructor.
     *
     * @param command Command.
     * @param args Arguments.
     * @param workDir Working directory.
     */
    WinProcess(std::string command, std::vector<std::string> args, std::string workDir)
        : m_running(false)
        , m_command(std::move(command))
        , m_args(std::move(args))
        , m_workDir(std::move(workDir))
        , m_info{} {}

    /**
     * Destructor.
     */
    ~WinProcess() override { kill(); }

    /**
     * Start process.
     */
    bool start() final {
        if (m_running)
            return false;

        STARTUPINFO si;

        std::memset(&si, 0, sizeof(si));
        si.cb = sizeof(si);
        std::memset(&m_info, 0, sizeof(m_info));

        std::stringstream fullCmd;
        fullCmd << m_command;
        for (auto &arg : m_args) {
            fullCmd << " " << arg;
        }

        auto fullCmdStr = fullCmd.str();

        std::vector<char> cmd(fullCmdStr.begin(), fullCmdStr.end());
        cmd.push_back(0);

        BOOL success = CreateProcess(NULL, cmd.data(), NULL, NULL, FALSE, 0, NULL, m_workDir.c_str(), &si, &m_info);

        m_running = success == TRUE;

        return m_running;
    }

    /**
     * Kill the process.
     */
    void kill() final {
        if (!m_running)
            return;

        std::vector<DWORD> processTree = getProcessTree(m_info.dwProcessId);
        for (auto procId : processTree) {
            HANDLE hChildProc = ::OpenProcess(PROCESS_ALL_ACCESS, FALSE, procId);
            if (hChildProc) {
                TerminateProcess(hChildProc, 1);
                CloseHandle(hChildProc);
            }
        }

        TerminateProcess(m_info.hProcess, 1);

        CloseHandle(m_info.hProcess);
        CloseHandle(m_info.hThread);

        m_running = false;
    }

    /**
     * Join process.
     *
     * @param timeout Timeout.
     */
    void join(std::chrono::milliseconds timeout) final {
        auto msecs = timeout.count() < 0 ? INFINITE : static_cast<DWORD>(timeout.count());

        WaitForSingleObject(m_info.hProcess, msecs);
    }

private:
    /** Running flag. */
    bool m_running;

    /** Command. */
    const std::string m_command;

    /** Arguments. */
    const std::vector<std::string> m_args;

    /** Working directory. */
    const std::string m_workDir;

    /** CmdProcess information. */
    PROCESS_INFORMATION m_info;
};

} // namespace ignite::detail
