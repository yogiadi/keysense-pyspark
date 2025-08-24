from setuptools import setup, find_packages

setup(
    name="keysense",
    version="0.4.2",
    description="KeySense: detect and score dataset grain (candidate keys) in PySpark",
    author="Aditya Yogi",
    url="https://github.com/yogiadi/keysense-pyspark",
    packages=find_packages(exclude=["tests*", "examples*"]),
    install_requires=[
        "pyspark>=3.0.0",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
