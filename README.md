# os-rq-hub

[![Build Status](https://www.travis-ci.org/cfhamlet/os-rq-hub.svg?branch=master)](https://www.travis-ci.org/cfhamlet/os-rq-hub)
[![codecov](https://codecov.io/gh/cfhamlet/os-rq-hub/branch/master/graph/badge.svg)](https://codecov.io/gh/cfhamlet/os-rq-hub)
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/cfhamlet/os-rq-hub?tab=overview)

Request queue hub for broad crawls


## Install

You can get the library with ``go get``

```
go get -u github.com/cfhamlet/os-rq-hub
```

The binary command line tool can be build from source, you should always build the latest released version , [Releases List](https://github.com/cfhamlet/os-rq-hub/releases)

```
git clone -b v0.0.1 https://github.com/cfhamlet/os-rq-hub.git
cd os-rq-hub
make install
```

## Usage

### Command line

```
$ rq-hub -h
rq-hub command line tool

Usage:
  rq-hub [flags]
  rq-hub [command]

Available Commands:
  help        Help about any command
  run         run rq-hub server
  version     show version info

Flags:
  -h, --help   help for rq-hub

Use "rq-hub [command] --help" for more information about a command.
```

## License
  MIT licensed.

