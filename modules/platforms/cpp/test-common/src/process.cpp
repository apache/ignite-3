/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef WIN32
#   include <windows.h>
#   include <tlhelp32.h>
#endif

#include <filesystem>
#include <utility>
#include <vector>

#include "process.h"

namespace
{
#ifdef WIN32
    /**
     * Get process tree.
     * @param processId ID of the parent process.
     * @return Process tree.
     */
    std::vector<DWORD> getProcessTree(DWORD processId) // NOLINT(misc-no-recursion)
    {
        std::vector<DWORD> children;
        PROCESSENTRY32 pe;

        memset(&pe, 0, sizeof(PROCESSENTRY32));
        pe.dwSize = sizeof(PROCESSENTRY32);

        HANDLE hSnap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);

        if (Process32First(hSnap, &pe))
        {
            BOOL bContinue = TRUE;

            while (bContinue)
            {
                if (pe.th32ParentProcessID == processId)
                    children.push_back(pe.th32ProcessID);

                bContinue = Process32Next(hSnap, &pe);
            }
        }

        std::vector<DWORD> tree(children);
        for (auto procId : children)
        {
            std::vector<DWORD> childTree = getProcessTree(procId);
            tree.insert(tree.end(), childTree.begin(), childTree.end());
        }

        return tree;
    }

    /**
     * Implementation of Process for Windows.
     */
    class WinProcess : public ignite::Process
    {
    public:
        /**
         * Constructor.
         *
         * @param command Command.
         * @param workDir Working directory.
         */
        WinProcess(std::string command, std::string workDir) :
            running(false),
            command(std::move(command)),
            workDir(std::move(workDir)),
            info{}
        { }

        /**
         * Destructor.
         */
        ~WinProcess() override = default;


        /**
         * Start process.
         */
        bool start() override
        {
            if (running)
                return false;

            STARTUPINFO si;

            std::memset(&si, 0, sizeof(si));
            si.cb = sizeof(si);
            std::memset(&info, 0, sizeof(info));

            std::vector<char> cmd(command.begin(), command.end());
            cmd.push_back(0);

            BOOL success = CreateProcess(
                    NULL, cmd.data(), NULL, NULL,
                    FALSE, 0, NULL, workDir.c_str(),
                    &si, &info);

            running = success == TRUE;

            return running;
        }

        /**
         * Kill the process.
         */
        void kill() override
        {
            std::vector<DWORD> processTree = getProcessTree(info.dwProcessId);
            for (auto procId : processTree)
            {
                HANDLE hChildProc = ::OpenProcess(PROCESS_ALL_ACCESS, FALSE, procId);
                if (hChildProc)
                {
                    TerminateProcess(hChildProc, 1);
                    CloseHandle(hChildProc);
                }
            }

            TerminateProcess(info.hProcess, 1);

            CloseHandle( info.hProcess );
            CloseHandle( info.hThread );
        }

        /**
         * Join process.
         *
         * @param timeout Timeout.
         */
        void join(std::chrono::milliseconds timeout) override
        {
            auto msecs = timeout.count() < 0 ? INFINITE : static_cast<DWORD>(timeout.count());

            WaitForSingleObject(info.hProcess, msecs);
        }

    private:
        /** Running flag. */
        bool running;

        /** Command. */
        const std::string command;

        /** Working directory. */
        const std::string workDir;

        /** Process information. */
        PROCESS_INFORMATION info;
    };

#else // #ifdef WIN32

#endif // #ifdef WIN32
}

namespace ignite
{
    std::unique_ptr<Process> Process::make(std::string command, std::string workDir)
    {
#ifdef WIN32
        return std::unique_ptr<Process>(new WinProcess(std::move(command), std::move(workDir)));
#else
#endif
    }
} // namespace ignite