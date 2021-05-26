from setuptools import setup, find_packages

setup(name='blitzen',
      version='0.0.1',
      packages=find_packages(),
      description='Multiprocessing and Distributed computing dispatcher toolkit.',
      author = 'Blake Richey',
      author_email='blake.e.richey@gmail.com',
      install_requires=[
        'dill>=0.3.3',
      ],
    )