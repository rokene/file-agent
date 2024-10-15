# fileagent

This script will download a particular shared folder with `id` and place it into the dedicated folder.

## PREREQ
* Python 3.13.0

## USAGE

> This requires [credentials.json](./credentials.json) an OAuth 2.0 from https://console.cloud.google.com/apis/credentials on the account you wish to work with.

Modify [config.json](./config.json) to customize shared folders.

Setup: `py -m pip install -r requirements.txt`

Execute the script: `py fileagent.py`

## Known Issues

* SSL Version errors occur when congiured to use more than one worker.
