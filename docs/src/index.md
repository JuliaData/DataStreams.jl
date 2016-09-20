# DataStreams.jl

The `DataStreams.jl` package aims to define a generic and performant framework for the transfer of "table-like" data. (i.e. data that can, at least in some sense, be described by rows and columns).

The framework achieves this by defining interfaces (i.e. a group of methods) for `Data.Source` types and methods to describe how they "provide" data; as well as `Data.Sink` types and methods around how they "receive" data. This allows `Data.Source`s and `Data.Sink`s to implement their interfaces separately, without needing to be aware of each other. The end result is an ecosystem of packages that "automatically" talk with each other, with adding an additional package not requiring changes to existing packages.

Packages can have a single julia type implement both the `Data.Source` and `Data.Sink` interfaces, or two separate types can implement them separately. For examples of interface implementations, see some of the packages below:

`Data.Source` implementations:
  * [`CSV.Source`](https://github.com/JuliaData/CSV.jl/blob/master/src/Source.jl)
  * [`SQLite.Source`](https://github.com/JuliaDB/SQLite.jl/blob/master/src/Source.jl)
  * [`DataFrame`](https://github.com/JuliaData/DataStreams.jl/blob/master/src/DataStreams.jl#L164)
  * [`ODBC.Source`](https://github.com/JuliaDB/ODBC.jl/blob/master/src/Source.jl)

`Data.Sink` implementations:
  * [`CSV.Sink`](https://github.com/JuliaData/CSV.jl/blob/master/src/Sink.jl)
  * [`SQLite.Sink`](https://github.com/JuliaDB/SQLite.jl/blob/master/src/Sink.jl)
  * [`DataFrame`](https://github.com/JuliaData/DataStreams.jl/blob/master/src/DataStreams.jl#L182)
  * [`ODBC.Sink`](https://github.com/JuliaDB/ODBC.jl/blob/master/src/Sink.jl)

## `Data.Source` Interface

The `Data.Source` interface requires the following definitions, where `MyPkg` would represent a package wishing to implement the interface:

  * `Data.schema(::MyPkg.Source) => Data.Schema`; get the `Data.Schema` of a `Data.Source`. Typically the `Source` type will store the `Data.Schema` directly, but this isn't strictly required. See `?Data.Schema` or docs below for more information on `Data.Schema`
  * `Data.isdone(::MyPkg.Source, row, col) => Bool`; indicates whether the `Data.Source` will be able to provide a value at a given a `row` and `col`.

Optional definition:

  * `Data.reference(::MyPkg.Source) => Vector{UInt8}`; Sometimes, a `Source` needs the `Sink` to keep a reference to memory to keep a data structure valid. A `Source` can implement this method to return a `Vector{UInt8}` that the `Sink` will need to handle appropriately.

A `Data.Source` also needs to "register" the type (or types) of streaming it supports. Currently defined streaming types in the DataStreams framework include:

  * `Data.Field`: a field is the intersection of a specific row and column; this type of streaming will traverse the "table" structure by row, accessing each column on each row
  * `Data.Column`: this type of streaming will provide entire columns at a time

A `Data.Source` formally supports **field-based** streaming by defining the following:

  * `Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Field}) = true`; declares that `MyPkg.Source` supports field-based streaming
  * `Data.getfield{T}(::MyPkg.Source, ::Type{Nullable{T}}, row, col) => Nullable{T}`; returns a value of type `Nullable{T}` given a specific `row` and `col` from `MyPkg.Source`
  * `Data.getfield{T}(::MyPkg.Source, ::Type{T}, row, col) => T`; returns a value of type `T` given a specific `row` and `col` from `MyPkg.Source`

And for column-based streaming:

  * `Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Column}) = true`  
  * `Data.getcolumn{T}(::Data.Source, ::Type{T}, col) => Vector{T}`; Given a type `T`, returns column # `col` of a `Data.Source` as a `Vector{T}`
  * `Data.getcolumn{T}(::Data.Source, ::Type{Nullable{T}}, col) => NullableVector{T}`; Given a type `Nullable{T}`, returns column # `col` of a `Data.Source` as a `NullableVector{T}`

## `Data.Sink` Interface

Similar to a `Data.Source`, a `Data.Sink` needs to "register" the types of streaming it supports, it does so through the following definition:

  * `Data.streamtypes(::Type{MyPkg.Sink}) = [Data.Field[, Data.Column]]`; "registers" the streaming preferences for `MyPkg.Sink`. A `Sink` type should list the stream type or types it supports. If the `Sink` supports streaming of multiple types, it should list them in order of preference (i.e. the more natural or performant type first).

A `Data.Sink` should also implement specific forms of constructors that allow convenience in many higher-level streaming functions:

  * `MyPkg.Sink{T <: Data.StreamType}(source, ::Type{T}, append::Bool, args...)`; given an instance of a `Data.Source`, the type of streaming `T`, whether the user desires to append `source` or not, and any necessary `args...`, construct an appropriate instance of `MyPkg.Sink` ready to receive data from `source`. The `append` argument allows an already existing sink file/source to "reset" itself if the user does not desire to append.
  * `MyPkg.Sink{T <: Data.StreamType}(sink, source, ::Type{T}, append::Bool)`; similar to above, but instead of constructing a new `Sink`, an existing `Sink` is given as a first argument, which may be modified before being returned, ready to receive data from `source`.

Similar to `Data.Source`, a `Data.Sink` also needs to implement it's own `stream` method that indicates how it receives data.

A `Data.Sink` supports **field-based** streaming by optionally defining:

  * OPTIONAL: `Data.open!(sink::MyPkg.Sink, source)`: typically, any necessary `Data.Sink` setup should be accomplished in it's own constructors (`MyPkg.Sink()` methods), but there are also cases tied specifically to the streaming process where certain actions need to be taken right before streaming begins. If a `Data.Sink` needs to perform this kind of action, it can overload `Data.open!(sink::MyPkg.Sink, source)`
  * OPTIONAL: `Data.cleanup!(sink::MyPkg.Sink)`: certain `Data.Sink`, like databases, may need to protect against inconvenient or dangerous "states" if there happens to be an error while streaming. `Data.cleanup!` provides the sink a way to rollback a transaction or other kind of cleanup if an error occurs during streaming
  * OPTIONAL: `Data.flush!(sink::MyPkg.Sink)`: similar to `Data.open!`, a `Data.Sink` may wish to perform certain actions once the streaming of a single `Data.Source` has finished. Note however, that a `Data.Sink` should still be ready to receive more data after a call to `Data.stream!` has finished. Only once a call to `Data.close!(sink)` has been made should a sink fully commit/close resources for good.
  * OPTIONAL: `Data.close!(sink::MyPkg.Sink)`: as noted above, `Data.close!` is defined to allow a sink to fully commit all streaming results and close/destroy any necessary resources.

The only method that is absolutely required is:

  * REQUIRED: `Data.streamfield!{T}(sink::MyPkg.Sink, source, ::Type{T}, row, col, cols[, sinkrows])`: Given a `row` and `col`, a `Data.Sink` should first call `Data.getfield(source, T, row, col)` to get the `Data.Source` value for that `row` and `col`, and then store the value appropriately. The type of the value retrieved is given by `T`, which may be `Nullable{T}`. Also provided are the total number of columns `cols` as well as the number of rows a sink began with as `sinkrows`. These arguments are passed for efficiency since they can be calculated once at the beginning of a `Data.stream!` and used quickly for many calls to `Data.streamfield!`.

A `Data.Sink` supports **column-based** streaming by defining:

  * `Data.streamcolumn!{T}(sink::MyPkg.Sink, source, ::Type{T}, col, row) => # of rows streamed`: Given a column number `col`, a `Data.Sink` should first call `Data.getcolumn(source, T, col)` to receive the column of data from the `Data.Source` before storing it appropriately. The type of the column is given by `T`. Also provided is the number of rows `row` that have been streamed so far. This method should return the # of rows that were present in the column streamed from `Data.Source` so that the total # of streamed rows can be tracked accurately.

## `Data.Schema`

```@docs
Data.Schema
```
