import pathlib
from setuptools import setup, find_packages

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(name='blitzen',
      version='1.0.0',
      packages=find_packages(),
      description='Multiprocessing and Distributed computing dispatcher toolkit.',
      long_description=README,
      long_description_content_type="text/markdown",
      author = 'Blake Richey',
      author_email='blake.e.richey@gmail.com',
      url='https://github.com/BlakeERichey/blitzen',
      license='MIT',
      install_requires=[
        'dill>=0.3.3',
      ],
    )