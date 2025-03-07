#!/usr/bin/env python3

from setuptools import find_packages, setup

setup(
    name="ard-scene-select",
    version="0.2.0",
    description="Select scenes to be processed by ARD.",
    keywords="ard-scene-select",
    url="https://github.com/GeoscienceAustralia/dea-ard-scene-select",
    license="Apache License 2.0",
    long_description=open("README.rst", encoding="utf-8").read(),
    platforms="any",
    zip_safe=False,
    classifiers=[
        "Development Status :: 1 - Planning",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=("tests", "modules", "examples")),
    include_package_data=True,
    install_requires=open("requirements.txt").read().strip().split("\n"),
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    entry_points={
        "console_scripts": [
            "ard-scene-select = scene_select.ard_scene_select:scene_select",
            "ard-bulk-reprocess = scene_select.bulk_process:cli",
            "ard-bulk-merge = scene_select.merge_bulk_runs:cli",
            "generate-aoi = scene_select.generate_aoi:generate_region",
            "ard-reprocessed-l1s = scene_select.ard_reprocessed_l1s:ard_reprocessed_l1s",
        ]
    },
)
