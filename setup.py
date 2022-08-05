from setuptools import setup

setup(
    name='tda_access',
    version='0.0.1',
    packages=['tda_access'],
    url='',
    license='',
    author='bjahnke',
    author_email='bjahnke71@gmail.com',
    description='interface for tda api and stream',
    install_requires=[
        'tda-api',
        'numpy',
        'pandas',
        'selenium',
        'authlib',
        'abstract_broker @ git+https://github.com/bjahnke/abstract_broker.git#egg=abstract_broker',
    ]
)
