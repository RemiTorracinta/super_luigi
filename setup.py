
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
