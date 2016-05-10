from setuptools import setup, find_packages
from trex import __version__


setup(
    author="Anthony Almarza",
    name="trex",
    packages=find_packages(exclude=["tests*", ]),
    version=__version__,
    url="https://github.com/anthonyalmarza/trex",
    download_url=(
        "https://github.com/anthonyalmarza/trex/tarball/" + __version__
    ),
    license="MIT",
    description="twisted redis",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: POSIX',
        'Programming Language :: Python',
    ],
    keywords=["twisted", "redis"],
    install_requires=[
        'twisted',
        'hiredis',
    ],
    extras_require={
        'dev': ['ipdb', 'mock', 'tox', 'coverage'],
    }
)
