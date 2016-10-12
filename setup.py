# Copyright (c) 2012 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os

try:
    from setuptools import setup
except:
    from distutils.core import setup

setup(
    name='super_luigi',
    version='0.0.1',
    description='Luigi extensions',
    long_description="Luigi extensions for protocol and custome runtimes",
    author='Mingming Sun',
    author_email='sunmingming@gmail.com',
    url='https://github.com/rudaoshi/super_luigi',
    license='MIT License',
    packages=[
        'super_luigi',
        'super_luigi.job',
        'super_luigi.protocol'

    ]
)
