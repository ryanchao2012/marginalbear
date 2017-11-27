from setuptools import setup, find_packages
import os

__version__ = "0.1.0"
__project__ = "marginalbear"
__author__ = "okcomputer"
__email__ = "<ryanchao2012@gmail.com>"
__maintainer__ = "Ryan Chao"
__license__ = "MIT"


def _read(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    try:
        file = open(path, encoding='utf-8')
    except TypeError:
        file = open(path)
    return file.read()


setup(
    name=__project__,
    version=__version__,
    description=_read('DESCRIPTION'),
    long_description=_read('README'),
    license=__license__,
    author=__author__,
    author_email=__email__,
    maintainer=__maintainer__,
    maintainer_email=__email__,
    url='https://github.com/ryanchao2012/marginalbear.git',
    keywords='chatbot ptt',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Software Development',
    ],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
)
