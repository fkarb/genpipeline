"""
Setup for pipeline

Setup script for building pipeline package
"""

from setuptools import find_packages, setup

setup_params = dict(
    name="pipeline",
    description="pipeline Python package",
    packages=find_packages(),
    test_suite = "nose.collector",
    version = "0.1.0",
    install_requires = ["greenlet>=0.4.0", "sqlalchemy>=0.7.0"],
    tests_require = ["nose>=1.2.1"],
)

if __name__ == '__main__':
    setup(**setup_params)
