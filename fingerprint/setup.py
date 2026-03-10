from setuptools import setup, find_packages

setup(
    name="telescope",
    version="4.3.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "av",
        "numpy",
        "scipy",
        "xxhash",
        "redis",
        "click",
    ],
)
