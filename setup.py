from setuptools import find_packages, setup

setup(
    name="LA2028",
    packages=find_packages(exclude=["LA2028_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
