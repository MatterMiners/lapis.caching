# Simulating Caching for Distributed Opportunistic Resources

LAPIS.caching is an extension for the [LAPIS](https://github.com/MatterMiners/lapis)
simulator.
The extension adds components required to simulate various caching scenarios
on top of a simulation of distributed opportunistic resources scheduling.

It provides the following features:

* Reading configuration for simulation setup from CLI and CSV as well as JSON files.
* Functionality for hitrate based caching.
* Basic functionality for different types of storage elements and their interconnects.
* Configurable monitoring for caching related results.

LAPIS.caching targets *scientists* who want to research (opportunistic) caching
scenarios specifically in HTCondor-based scheduling environments.
However, the LAPIS core is extensible and other schedulers can easily be implemented
giving *Python developers* an opportunity to integrate their use cases.

    The current version is still in a development stage and therefore does not cover
    the whole functionality required to simulate all aspects of caching.
    Please feel invited to submit your contributions and ideas.

## Installation

LAPIS.Caching is currently in development stage.
To setup the package and contribute please ensure to

* clone this repository.

Further, we use [Poetry](https://python-poetry.org) to properly
setup the namespace dependency with
[LAPIS](https://github.com/MatterMiners/lapis).
So please ensure to set your working directory to the just
cloned repository and install the appropriate dependencies:

```bash
poetry install
```
