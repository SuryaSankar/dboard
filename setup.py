#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=7.0',
    "toolspy>=0.3.1",
    "Flask>=1.0.2",
    "SQLAlchemy>=1.3.1",
    "Flask-SQLAlchemy>=2.3.2",
    "flask_sqlalchemy_session",
    "Schemalite>=0.2.1",
    "bleach",
    "pandas",
    "numpy",
    "flask_sqlalchemy_booster",
    "mysqlclient"
]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Surya Sankar",
    author_email='suryashankar.m@gmail.com',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Tools to help create a data dashboard",
    entry_points={
        'console_scripts': [
            'databuddy=databuddy.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='databuddy',
    name='databuddy',
    packages=find_packages(include=['databuddy', 'databuddy.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/suryasankar/databuddy',
    version='0.1.12',
    zip_safe=False,
)
