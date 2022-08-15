# Textbook

## Setup Development Environment
The project uses `pipenv` for managing dependencies and `Click` for providing the command line interface.

Make sure you have pipenv installed on your system. To install the project and its dependencies, run

```
pipenv install -e .
```

Enter the virtual environment

```
pipenv shell
```

Then you should be able to access the `textbook` cli in a `hote-reload` mode and print help with:

```
textbook
```

