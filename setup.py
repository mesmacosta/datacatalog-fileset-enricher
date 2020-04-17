from setuptools import find_packages, setup

with open('README.md') as readme_file:
    readme = readme_file.read()

setup(
    name='datacatalog-fileset-enricher',
    version='1.1.0',
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
    python_requires='>=3.6',
    tests_require=(
        'pytest-cov'
    ),
    long_description=readme,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    url='https://github.com/mesmacosta/datacatalog-fileset-enricher',
    zip_safe=False,
)
