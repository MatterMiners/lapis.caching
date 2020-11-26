import setuptools

with open("README.rst", "r") as readme:
    long_description = readme.read()

setuptools.setup(
    name="lapis.caching",
    version="0.0.1",
    author="Eileen Kuehn, Max Fischer",
    author_email="mainekuehn@gmail.com",
    description="LAPIS extension to simulate caching",
    long_description=long_description,
    url="https://github.com/MatterMiners/lapis.caching",
    keywords="caching simulation cobald tardis opportunistic scheduling scheduler",
    # Even though `lapis` is not a namespace package, declaring `lapis.caching`
    # as one allows to drop `.caching` into `.lapis`. The result works as one.
    packages=setuptools.find_namespace_packages(include=['lapis.*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Development Status :: 2 - Pre-Alpha",
    ],
    install_requires=['lapis-sim'],
    python_requires='>=3.6',
)
