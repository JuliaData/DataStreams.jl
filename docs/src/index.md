# DataStreams.jl

The `DataStreams.jl` package aims to define a generic and performant framework for the transfer of "table-like" data. (i.e. data that can, at least in some sense, be described by rows and columns).

The framework achieves this by defining interfaces (i.e. a group of methods) for `Data.Source` types and methods to describe how they "provide" data; as well as `Data.Sink` types and methods around how they "receive" data. This allows `Data.Source`s and `Data.Sink`s to implement their interfaces separately, without needing to be aware of each other. The end result is an ecosystem of packages that "automatically" talk with each other, with adding an additional package not requiring changes to existing packages.

Packages can have a single julia type implement both the `Data.Source` and `Data.Sink` interfaces, or two separate types can implement them separately. 


## `Data.Source` Interface

The `Data.Source` interface includes the following definitions:

```@docs
Data.schema
Data.isdone
Data.streamtype
Data.reset!
Data.streamfrom
Data.accesspattern
Data.reference
```

## `Data.Sink` Interface

```@docs
Data.Sink
Data.streamtypes
Data.streamto!
Data.cleanup!
Data.close!
Data.weakrefstrings
```

## `Data.stream!`

```@docs
Data.stream!
```

## `Data.Schema`

```@docs
Data.Schema
```

The reference DataStreams interface implementation lives in the [DataStreams.jl package itself](https://github.com/JuliaData/DataStreams.jl/blob/master/src/DataStreams.jl#L370), implemented for a NamedTuple with AbstractVector elements.

For examples of additional interface implementations, see some of the packages below:

`Data.Source` implementations:
  * [`CSV.Source`](https://github.com/JuliaData/CSV.jl/blob/master/src/Source.jl)
  * [`SQLite.Source`](https://github.com/JuliaDB/SQLite.jl/blob/master/src/Source.jl)
  * [`ODBC.Source`](https://github.com/JuliaDB/ODBC.jl/blob/master/src/Source.jl)

`Data.Sink` implementations:
  * [`CSV.Sink`](https://github.com/JuliaData/CSV.jl/blob/master/src/Sink.jl)
  * [`SQLite.Sink`](https://github.com/JuliaDB/SQLite.jl/blob/master/src/Sink.jl)
  * [`ODBC.Sink`](https://github.com/JuliaDB/ODBC.jl/blob/master/src/Sink.jl)
