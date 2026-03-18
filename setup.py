from setuptools import setup, find_packages

setup(
    name="pymonque",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pymongo",
        "pydantic"
    ],
    author="PolarisWater",
    author_email="",
    description="Modular task queue using MongoDB",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/PolarisWater/pymonque",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.11",
)
