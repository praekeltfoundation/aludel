from setuptools import setup

setup(
    name="aludel",
    version="0.2.0",
    url='https://github.com/praekelt/aludel',
    license='MIT',
    description="A framework for RESTful services using Klein and Alchimia.",
    long_description=open('README.rst', 'r').read(),
    author='Praekelt Foundation',
    author_email='dev@praekeltfoundation.org',
    packages=["aludel", "aludel.tests"],
    install_requires=["Twisted", "klein", "sqlalchemy", "alchimia>=0.4"],
)
