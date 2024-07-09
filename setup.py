from setuptools import setup, find_packages

setup(
    name='stimula',
    version='0.0.1',
    description='Toolset library for Simple Table Mapping Language (STML)',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/stmlio/stimula',
    author='Romke Jonker',
    author_email='romke@rnadesign.net',
    packages=find_packages(),
    # py_modules=['stimula.cli.cli'],
    install_requires=[
        'pyjwt',
        'sqlalchemy',
        'pandas',
        'numpy',
        'cryptography>=3.4.8,<4.0',
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
