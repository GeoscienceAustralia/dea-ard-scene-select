#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import io
from setuptools import setup, find_packages


setup(
    name='ard-scene-select',
    description='Select scenes to be processed by ARD.',
    keywords='ard-scene-select',
    url='https://github.com/GeoscienceAustralia/landsat_sentinel2_scene_filter',
    license='Apache License 2.0',
    long_description=io.open(
        'README.rst', 'r', encoding='utf-8').read(),
    platforms='any',
    zip_safe=False,
    classifiers=[
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Operating System :: OS Independent',
    ],
    packages=find_packages(exclude=('tests',)),
    include_package_data=True,
    install_requires=open('requirements.txt').read().strip().split('\n'),
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    entry_points={
        'console_scripts': [
            'ard-scene-select = ard_scene_select.ard_scene_select:main',
        ]
    },
)
