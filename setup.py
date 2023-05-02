from setuptools import find_packages, setup

setup(
    name='patsy-db',
    version='2.0.0',
    description='Command-line client for preservation asset tracking system (PATSy)',
    author='Joshua A. Westgard',
    author_email="westgard@umd.edu",
    platforms=["any"],
    license="Apache",
    url="https://github.com/umd-lib/patsy-db",
    packages=find_packages(),
    entry_points={
        'console_scripts': ['patsy=patsy.__main__:main']
        },
    install_requires=[i.strip() for i in open("requirements.txt").readlines()],
    python_requires='>=3.10',
    extras_require={  # Optional
       'dev': ['pycodestyle==2.7.0', 'mypy==0.910'],
       'test': ['pytest==7.1.3', 'pytest-cov==2.12.1', 'pytest-alembic==0.10.4'],
    }
)
