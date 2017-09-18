from setuptools import setup, find_packages

setup(
    name = "bjdb",
    version = '0.0.2',

    url = "https://github.com/bieberg0n/BJDB",
    author = "bjong",
    author_email = "biebergong@gmail.com",

    packages = find_packages(),
    include_package_data = True,
    platforms = "any",
    install_requires = ["tinydb"]
)
