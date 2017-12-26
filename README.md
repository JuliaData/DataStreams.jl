
# DataStreams

*A fast, generic framework for transferring table-like data structures*

| **Documentation**                                                               | **PackageEvaluator**                                            | **Build Status**                                                                                |
|:-------------------------------------------------------------------------------:|:---------------------------------------------------------------:|:-----------------------------------------------------------------------------------------------:|
| [![][docs-stable-img]][docs-stable-url] [![][docs-latest-img]][docs-latest-url] | [![][pkg-0.6-img]][pkg-0.6-url] [![][pkg-0.7-img]][pkg-0.7-url] | [![][travis-img]][travis-url] [![][appveyor-img]][appveyor-url] [![][codecov-img]][codecov-url] |


## Installation

The package is registered in `METADATA.jl` and so can be installed with `Pkg.add`.

```julia
julia> Pkg.add("DataStreams")
```

## Documentation

- [**STABLE**][docs-stable-url] &mdash; **most recently tagged version of the documentation.**
- [**LATEST**][docs-latest-url] &mdash; *in-development version of the documentation.*

## Project Status

The package is tested against Julia `0.6` and *current* `0.7`/`1.0` on Linux, OS X, and Windows.

## Contributing and Questions

Contributions are very welcome, as are feature requests and suggestions. Please open an
[issue][issues-url] if you encounter any problems or would just like to ask a question.

## List of Known Implementations

* [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl)
* [CSV.jl](https://github.com/JuliaData/CSV.jl)
* [Feather.jl](https://github.com/JuliaData/Feather.jl)
* [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl)
* [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl)
* [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl)

[docs-latest-img]: https://img.shields.io/badge/docs-latest-blue.svg
[docs-latest-url]: https://JuliaData.github.io/DataStreams.jl/latest

[docs-stable-img]: https://img.shields.io/badge/docs-stable-blue.svg
[docs-stable-url]: https://JuliaData.github.io/DataStreams.jl/stable

[travis-img]: https://travis-ci.org/JuliaData/DataStreams.jl.svg?branch=master
[travis-url]: https://travis-ci.org/JuliaData/DataStreams.jl

[appveyor-img]: https://ci.appveyor.com/api/projects/status/h227adt6ovd1u3sx/branch/master?svg=true
[appveyor-url]: https://ci.appveyor.com/project/JuliaData/documenter-jl/branch/master

[codecov-img]: https://codecov.io/gh/JuliaData/DataStreams.jl/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/JuliaData/DataStreams.jl

[issues-url]: https://github.com/JuliaData/DataStreams.jl/issues

[pkg-0.6-img]: http://pkg.julialang.org/badges/DataStreams_0.6.svg
[pkg-0.6-url]: http://pkg.julialang.org/?pkg=DataStreams
[pkg-0.7-img]: http://pkg.julialang.org/badges/DataStreams_0.7.svg
[pkg-0.7-url]: http://pkg.julialang.org/?pkg=DataStreams
