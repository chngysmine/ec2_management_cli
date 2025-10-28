from setuptools import setup, find_packages

setup(
    name="ec2-manager",
    version="0.1.0",
    description="EC2 Management CLI for lifecycle, inventory, and cost optimization",
    author="",
    packages=find_packages(exclude=("tests", "docs")),
    install_requires=[
        "boto3~=1.34.0",
        "click~=8.1.0",
        "PyYAML~=6.0",
    ],
    python_requires=">=3.11",
    entry_points={
        "console_scripts": [
            "ec2-man=ec2_manager.cli:main_cli",
        ]
    },
    include_package_data=True,
    zip_safe=False,
)
