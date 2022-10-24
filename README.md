# Radiochild repmeta

## Build and Install
Radiochild repmeta is a go module that can be built and installed locally  

    $ go env -w GONOSUMDB=github.com/radiochild/*
    $ go get github.com/radiochild/utils
    $ go mod tidy
    $ go install ./...
  
## Usage
To refer to packages in this module as follows:

    import (
      "github.com/radiochild/repmeta"
    )
