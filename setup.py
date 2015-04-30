"""
Setup for genpipeline

Setup script for building genpipeline package
"""

from setuptools import find_packages, setup

setup_params = dict(
    name="genpipeline",
    description="A simple Python coroutine-based method for creating data processing pipelines",
    packages=find_packages(),
    test_suite = "nose.collector",
    version = "0.1.2",
    install_requires = ["greenlet>=0.4.0", "sqlalchemy>=0.7.0"],
    tests_require = ["nose>=1.2.1"],
    author = "Renshaw Bay",
    author_email = "technology@renshawbay.com",
    url="https://github.com/renshawbay/genpipeline",
    classifiers=["License :: OSI Approved :: MIT License"],
)

if __name__ == '__main__':
    setup(**setup_params)
