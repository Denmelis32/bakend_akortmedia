from setuptools import setup, find_packages

setup(
    name="shared",
    version="1.0.0",
    description="Shared library for microservices",
    author="Development Team",
    packages=find_packages(),
    install_requires=[
        "PyJWT>=2.0.0",
    ],
    python_requires=">=3.8",
)
