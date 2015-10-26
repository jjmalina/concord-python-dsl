from setuptools import setup, find_packages

setup(
    name='concord-python-dsl',
    version='0.1.0',
    url='https://github.com/jjmalina/concord-python-dsl',
    author='Jeremiah Malina',
    author_email='me@jerem.io',
    description='',
    long_description='',
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    install_requires=['concord-py==0.3.1'],
    tests_require=['pytest==2.8.2'],
    platforms='any'
)
