from setuptools import setup, find_packages

setup(
    name="crypto-pipeline",
    version="0.1.0",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "requests",
        "websocket-client",
    ],
    entry_points={
        "console_scripts": [
            "crypto-pipeline = crypto_pipeline.cli:main",
        ],
    },
)
