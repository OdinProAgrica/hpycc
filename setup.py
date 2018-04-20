from setuptools import setup

setup(name='hpycc',
      version='0.4.0',
      description='Download THOR files',
      author='Rob Mansfield',
      author_email='rob.mansfield@proagrica.com',
      license='GNU GPLv3',
      packages=['hpycc'],
      zip_safe=False,
      install_requires=[
          'pandas'
      ]
      )
