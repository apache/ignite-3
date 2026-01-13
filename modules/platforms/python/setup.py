# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import platform
import subprocess
import setuptools
import sys
import multiprocessing
from pprint import pprint
from setuptools.command.build_ext import build_ext
from setuptools.extension import Extension

PACKAGE_NAME = 'pyignite_dbapi'
EXTENSION_NAME = 'pyignite_dbapi._pyignite_dbapi_extension'


def is_a_requirement(req_line):
    return not any([
        req_line.startswith('#'),
        req_line.startswith('-r'),
        len(req_line) == 0,
    ])


install_requirements = []
with open('requirements/install.txt', 'r', encoding='utf-8') as requirements_file:
    for line in requirements_file.readlines():
        line = line.strip('\n')
        if is_a_requirement(line):
            install_requirements.append(line)

with open('README.md', 'r', encoding='utf-8') as readme_file:
    long_description = readme_file.read()

version = None
with open(os.path.join(PACKAGE_NAME, '_version.txt'), 'r') as fd:
    version = fd.read()
    if not version:
        raise RuntimeError('Cannot find version information')

def cmake_project_version(version):
    """
    Strips the pre-release portion of the project version string to satisfy CMake requirements
    """
    dash_index = version.find("-")
    if dash_index != -1:
        return version[:dash_index]
    return version

def _get_env_variable(name, default='OFF'):
    if name not in os.environ.keys():
        return default
    return os.environ[name]


# Command line flags forwarded to CMake (for debug purpose)
cmake_cmd_args = []
for f in sys.argv:
    if f.startswith('-D'):
        cmake_cmd_args.append(f)


class CMakeExtension(Extension):
    def __init__(self, name, cmake_lists_dir='.', sources=[], **kwa):
        Extension.__init__(self, name, sources=sources, **kwa)
        self.cmake_lists_dir = os.path.abspath(cmake_lists_dir)


class CMakeBuild(build_ext):
    def build_extensions(self):
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError('Cannot find CMake executable')

        for ext in self.extensions:
            ext_dir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
            cfg = 'Release'
            ext_file = os.path.splitext(os.path.basename(self.get_ext_filename(ext.name)))[0]

            cmake_args = [
                f'-DCMAKE_BUILD_TYPE={cfg}',
                f'-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{cfg.upper()}={ext_dir}',
                f'-DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_{cfg.upper()}={self.build_temp}',
                f'-DEXTENSION_FILENAME={ext_file}',
                f'-DIGNITE_VERSION={cmake_project_version(version)}',
                f'-DPython3_EXECUTABLE={sys.version_info}")'
            ]

            if platform.system() == 'Windows':
                plat = ('x64' if platform.architecture()[0] == '64bit' else 'Win32')
                cmake_args += [
                    '-DCMAKE_WINDOWS_EXPORT_ALL_SYMBOLS=TRUE',
                    f'-DCMAKE_RUNTIME_OUTPUT_DIRECTORY_{cfg.upper()}={ext_dir}',
                ]
                if self.compiler.compiler_type == 'msvc':
                    cmake_args += [
                        f'-DCMAKE_GENERATOR_PLATFORM={plat}',
                    ]
                else:
                    raise RuntimeError('Only MSVC is supported for Windows currently')

            cmake_args += cmake_cmd_args

            pprint(cmake_args)

            if not os.path.exists(self.build_temp):
                os.makedirs(self.build_temp)

            cpu_count = multiprocessing.cpu_count()

            # Config and build the extension
            subprocess.check_call(['cmake', ext.cmake_lists_dir] + cmake_args, cwd=self.build_temp)
            subprocess.check_call(['cmake', '--build', '.', '-j', str(cpu_count), '--config', cfg, '-v'],
                                  cwd=self.build_temp)


def run_setup():
    setuptools.setup(
        name=PACKAGE_NAME,
        version=version,
        python_requires='>=3.10',
        author='The Apache Software Foundation',
        author_email='dev@ignite.apache.org',
        description='Apache Ignite 3 DB API Driver',
        long_description=long_description,
        long_description_content_type='text/markdown',
        url='https://github.com/apache/ignite-3/tree/main/modules/platforms/python',
        packages=setuptools.find_packages(),
        include_package_data=True,
        ext_modules=[CMakeExtension(EXTENSION_NAME)],
        cmdclass=dict(build_ext=CMakeBuild),
        install_requires=install_requirements,
        license='Apache License 2.0',
        license_files=('LICENSE', 'NOTICE'),
        classifiers=[
            'Programming Language :: C++',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11',
            'Programming Language :: Python :: 3.12',
            'Programming Language :: Python :: 3.13',
            'Programming Language :: Python :: 3.14',
            'Programming Language :: Python :: 3 :: Only',
            'Intended Audience :: Developers',
            'Topic :: Database :: Front-Ends',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'Operating System :: MacOS',
            'Operating System :: Microsoft :: Windows',
            'Operating System :: POSIX :: Linux',
        ],
    )


if __name__ == "__main__":
    run_setup()
