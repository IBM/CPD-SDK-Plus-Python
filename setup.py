from setuptools import setup

setup(
   name='cpd_sdk_plus',
   version='1.1',
   author='Wendy Wang',
   author_email='wanting.wang@ibm.com',
   packages=['cpd_sdk_plus'],
   #scripts=['bin/script1','bin/script2'],
   url='https://github.ibm.com/dse-rnd-incubator/CPD-Utilities',
   license='LICENSE.txt',
   description='A colleciton of higher level utility methods for a range of services running on IBM Cloud Pak for Data. Some are based on the official SDK of the service.',
   long_description=open('README.txt').read(),
   install_requires=[
       "ibm-watson-machine-learning",
       "ibm-cloud-sdk-core==3.10.1",
       "ibm-watson-openscale>=3.0.14",
       "pytest",
   ],
)