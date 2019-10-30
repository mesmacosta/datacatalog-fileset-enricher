from setuptools import find_packages, setup

setup(
    name='datacatalog-fileset-enricher',
    version='0.0.1',
    author='Marcelo Costa',
    author_email='mesmacosta@gmail.com',
    description='A package for enriching the content of a fileset Entry with Datacatalog Tags',
    platforms='Posix; MacOS X; Windows',
    packages=find_packages(where='./src'),
    package_dir={
        '': 'src'
    },
    include_package_data=True,
    install_requires=(
        'pandas',
        'google-cloud-storage',
        'google-cloud-datacatalog',
    ),
    setup_requires=(
        'flake8',
        'pytest-runner',
    ),
    tests_require=(
        'pytest-cov'
    ),
    classifiers=[
        'Development Status :: 1 - Alpha',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
