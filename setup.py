from setuptools import setup, find_packages

setup(
    name="pension-fund-tracker",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
        "pdfplumber>=0.10.0",
        "openpyxl>=3.1.0",
        "pandas>=2.1.0",
        "rapidfuzz>=3.5.0",
        "click>=8.1.0",
        "pyyaml>=6.0",
        "python-dateutil>=2.8.0",
    ],
    entry_points={
        "console_scripts": [
            "pension-tracker=src.cli:cli",
        ],
    },
)
