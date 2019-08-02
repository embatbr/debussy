# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name='debussy',
    version='0.3.0',
    url='https://dotzmkt.visualstudio.com/DotzBigData/_git/debussy',
    author='Eduardo Tenório',
    author_email={
        'dotz': 'eduardo.tenorio@dotz.com',
        'personal': 'embatbr@gmail.com'
    },
    description='',
    packages=[
        'debussy',
        'debussy.operators'
    ],
    long_description=open('README.md').read()
)
