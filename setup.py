from setuptools import setup, find_packages

setup(
    name='pysockettaskqml',
    version='1.0.0',
    description='python implemented task queue framework for websocket based machine learning streaming application',
    author='Peter Chan',
    author_email='humblemat@gmail.com',
    package_dir={'': 'src'},
    packages=['.'],
    install_requires=[
      "protobuf"
    ],
)