from setuptools import setup, find_packages

setup(
    name='fiscam',
    version='0.1.0',
    description='A package for FISCAM processing',
    author='Stuart SCarton',
    author_email='Scarton_Stuart@bah.com',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'pytz',
        'datetime'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License',
        'Operating System :: OS Independent',
    ],
)