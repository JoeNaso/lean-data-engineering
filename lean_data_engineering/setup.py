from setuptools import find_packages, setup

setup(
    name="lean_data_engineering",
    packages=find_packages(exclude=["lean_data_engineering_tests"]),
    install_requires=[
        "dagster>=1.5.10",
        "dagster-cloud",
        "dagster-snowflake-pandas",
        "dagster-dbt",
        "dagster-aws",
        "dagster-shell",
        "dbt-snowflake",
        "duckdb",
        "pyarrow",
        "psycopg2-binary",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
    package_data={"lean_data_engineering": ["lib/*"]},
    include_package_data=True,
)
