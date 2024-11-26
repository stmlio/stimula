from setuptools import setup, find_packages

setup(
    name='stimula',
    version='1.2.7',
    description='Toolset library for Simple Table Mapping Language (STML)',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/stmlio/stimula',
    author='Romke Jonker',
    author_email='romke@stml.io',
    packages=find_packages(),
    install_requires=[
        'pyjwt',
        'sqlalchemy',
        'pandas>=2.1.3',
        'numpy>=1.22.4',
        'cryptography>=3.4.8',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'stimula = stimula.cli.cli:main',
        ],
    },
)
