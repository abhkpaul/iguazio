from distutils.command.install_data import install_data

from setuptools import setup, find_packages

setup(name="rx-perso-spark_iguazio",
      version="1.0",
      packages=find_packages(),
      package_data={'bin': ['configs/*.yml']},
      install_requires=[
          'pyspark'
          'mlrun'],
      cmdclass={'install_data': install_data})
