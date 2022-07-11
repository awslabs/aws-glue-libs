from setuptools import find_packages, setup

setup(
    name='aws-glue-libs',
    version='1.0.0',
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
)
