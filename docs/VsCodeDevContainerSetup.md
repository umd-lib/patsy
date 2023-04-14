# VS Code Dev Container Setup

## Introduction

This page describes how to use the "Dev Container" functionality in VS Code to
develop patsy.

This simplifies the setup of the application, and automatically provides:

* The correct Python environment for running the application
* Highlighting of "pycodestyle" violations in the editor
* Support of VS Code debugging

Using VS Code "Dev Containers" is intended as an alternative to the
"Local Development Setup" steps in [DevelopmentSetup.md](./DevelopmentSetup.md).
See "DevelopmentSetup.md" for additional information about the development
environment for the application.

## Useful Resources

* https://code.visualstudio.com/docs/devcontainers/containers

## Prerequisites

The following assumes that Docker and the "Dev Containers" extension
(ms-vscode-remote.remote-containers) is installed in local copy of VS Code
(see <https://code.visualstudio.com/docs/devcontainers/tutorial> for more
information).

## Dev Container Setup

1) Clone the "patsy" GitHub repository:

    ```bash
    $ git clone git@github.com:umd-lib/patsy.git
    ```

2) Switch to the "patsy" directory:

    ```bash
    $ cd patsy
    ```

3) Create a ".devcontainer" directory and switch into it:

    ```bash
    $ mkdir .devcontainer
    $ cd .devcontainer
    ```

4) Create a "devcontainer.json" file:

    ```bash
    $ vi devcontainer.json
    ```

    with the following content:

    ```text
    // For format details, see https://aka.ms/devcontainer.json. For config options, see the
    // README at: https://github.com/devcontainers/templates/tree/main/src/python
    {
      "name": "Python 3",
      // Or use a Dockerfile or Docker Compose file. More info: https://containers.dev/guide/dockerfile
      "image": "mcr.microsoft.com/devcontainers/python:0-3.10",

      // Features to add to the dev container. More info: https://containers.dev/features.
      // "features": {},

      // Use 'forwardPorts' to make a list of ports inside the container available locally.
      // "forwardPorts": [],

      // Use 'postCreateCommand' to run commands after the container is created.
      "postCreateCommand": "pip install -e .[dev,test]",

      // Configure tool-specific properties.
      "customizations": {
        "vscode": {
          "extensions": [
            "ms-python.python",
            "ms-python.vscode-pylance",
            "eamodio.gitlens"
          ],
          "settings": {
            "python.linting.pycodestyleEnabled": true,
            "python.testing.pytestEnabled": true,
            "python.testing.autoTestDiscoverOnSaveEnabled": true
          }
        }
      }

      // Uncomment to connect as root instead. More info: https://aka.ms/dev-containers-non-root.
      // "remoteUser": "root"
    }
    ```

    **Note:** This configuration uses the Docker image for Python 3.10
    ("mcr.microsoft.com/devcontainers/python:0-3.10"). It may need to be updated
    as the Python version used by patsy changes.

5) Run VS Code.

6) In VS Code, select "File | Open Folder..." from the menubar. In the resulting
   file dialog, select the "patsy" directory, and left-click the "OK" button.
   The "patsy" folder will be added to VS Code.

7) Once the "patsy" folder has been added, a notification will pop up
   asking if the folder should be re-opened in a container. Select "Yes".
   VS Code will restart and create a Docker container (this takes a minute
   or two, if the Docker image has not previously been downloaded).

   The `pip install -e .[dev,test]` command is run as part of the Docker
   container setup, and various VS Code extensions are automatically added.

8) To run Patsy commands and Python tools such as "pytest", open a terminal
   in VS Code (select "Terminal | New Terminal") from the menubar.

9) If you are running a local instance of Postgres (either from the local
    workstation, or in a separate Docker container), the "PATSY_DATABASE"
    environment variable should use "host.docker.internal" as the hostname,
    i.e.:

    ```bash
    $ export PATSY_DATABASE=postgresql+psycopg2://postgres:password@host.docker.internal:5432/patsy
    ```
