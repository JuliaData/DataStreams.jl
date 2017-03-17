# DataStreams.jl

The `DataStreams.jl` package aims to define a generic and performant framework for the transfer of "table-like" data. (i.e. data that can, at least in some sense, be described by rows and columns).

The framework achieves this by defining interfaces (i.e. a group of methods) for `Data.Source` types and methods to describe how they "provide" data; as well as `Data.Sink` types and methods around how they "receive" data. This allows `Data.Source`s and `Data.Sink`s to implement their interfaces separately, without needing to be aware of each other. The end result is an ecosystem of packages that "automatically" talk with each other, with adding an additional package not requiring changes to existing packages.

Packages can have a single julia type implement both the `Data.Source` and `Data.Sink` interfaces, or two separate types can implement them separately. For examples of interface implementations, see some of the packages below:

`Data.Source` implementations:
  * [`CSV.Source`](https://github.com/JuliaData/CSV.jl/blob/master/src/Source.jl)
  * [`SQLite.Source`](https://github.com/JuliaDB/SQLite.jl/blob/master/src/Source.jl)
  * [`DataFrame`](https://github.com/JuliaStats/DataFrames.jl/blob/master/src/abstractdataframe/io.jl)
  * [`DataTable`](https://github.com/JuliaData/DataTables.jl/blob/master/src/abstractdatatable/io.jl)
  * [`ODBC.Source`](https://github.com/JuliaDB/ODBC.jl/blob/master/src/Source.jl)

`Data.Sink` implementations:
  * [`CSV.Sink`](https://github.com/JuliaData/CSV.jl/blob/master/src/Sink.jl)
  * [`SQLite.Sink`](https://github.com/JuliaDB/SQLite.jl/blob/master/src/Sink.jl)
  * [`DataFrame`](https://github.com/JuliaStats/DataFrames.jl/blob/master/src/abstractdataframe/io.jl)
  * [`DataTable`](https://github.com/JuliaData/DataTables.jl/blob/master/src/abstractdatatable/io.jl)
  * [`ODBC.Sink`](https://github.com/JuliaDB/ODBC.jl/blob/master/src/Sink.jl)

## `Data.Source` Interface

The `Data.Source` interface requires the following definitions, where `MyPkg` would represent a package wishing to implement the interface:

  * `Data.isdone(::MyPkg.Source, row, col) => Bool`; indicates whether the `Data.Source` will be able to provide a value at a given a `row` and `col`.

Optional definition:

  * `Data.reference(::MyPkg.Source) => Vector{UInt8}`; Sometimes, a `Source` needs the `Sink` to keep a reference to memory to keep data valid after the `Source` goes out of scope. A `Source` can implement this method to return a `Vector{UInt8}` that the `Sink` will need to handle appropriately.
  * `Base.size(::MyPkg.Source[, i]) => Int`; not explicitly required to enable data-streaming, but a `Source` should generally be able to describe its first 2 dimensions, i.e. # of rows and columns.

A `Data.Source` also needs to "register" the type (or types) of streaming it supports. Currently defined streaming types in the DataStreams framework include:

  * `Data.Field`: a field is the intersection of a specific row and column; this type of streaming will traverse the "table" structure by row, accessing each column on each row
  * `Data.Column`: this type of streaming will provide entire columns at a time

A `Data.Source` formally supports **field-based** streaming by defining the following:

  * `Data.schema(::MyPkg.Source, ::Type{Data.Field}) => Data.Schema`; get the `Data.Schema` of a `Data.Source`. The column types of `MyPkg.Source` are provided as "scalar" types, so `Nullable{Int}` instead of `NullableVector{Int}`. Typically the `Source` type will store the `Data.Schema` directly, but this isn't strictly required. See `?Data.Schema` or docs below for more information on `Data.Schema`
  * `Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Field}) = true`; declares that `MyPkg.Source` supports field-based streaming
  * `Data.streamfrom{T}(::MyPkg.Source, ::Type{Data.Field}, ::Type{Nullable{T}}, row, col) => Nullable{T}`; returns a value of type `Nullable{T}` given a specific `row` and `col` from `MyPkg.Source`
  * `Data.streamfrom{T}(::MyPkg.Source, ::Type{Data.Field}, ::Type{T}, row, col) => T`; returns a value of type `T` given a specific `row` and `col` from `MyPkg.Source`

And for column-based streaming:

  * `Data.schema(::MyPkg.Source, ::Type{Data.Column}) => Data.Schema`; get the `Data.Schema` of a `Data.Source`. The column types of `MyPkg.Source` are provided as "vector" types, so `NullableVector{Int}` instead of `Nullable{Int}`. Typically the `Source` type will store the `Data.Schema` directly, but this isn't strictly required. See `?Data.Schema` or docs below for more information on `Data.Schema`
  * `Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Column}) = true`  
  * `Data.streamfrom{T}(::Data.Source, ::Type{Data.Column}, ::Type{T}, col) => Vector{T}`; Given a type `T`, returns column # `col` of a `Data.Source` as a `Vector{T}`
  * `Data.streamfrom{T}(::Data.Source, ::Type{Data.Column}, ::Type{Nullable{T}}, col) => NullableVector{T}`; Given a type `Nullable{T}`, returns column # `col` of a `Data.Source` as a `NullableVector{T}`

## `Data.Sink` Interface

Similar to a `Data.Source`, a `Data.Sink` needs to "register" the types of streaming it supports, it does so through the following definition:

  * `Data.streamtypes(::Type{MyPkg.Sink}) = [Data.Field[, Data.Column]]`; "registers" the streaming preferences for `MyPkg.Sink`. A `Sink` type should list the stream type or types it supports. If the `Sink` supports streaming of multiple types, it should list them in order of preference (i.e. the more natural or performant type first).

A `Data.Sink` needs to also implement specific forms of constructors that ensure proper Sink state in many higher-level streaming functions:

  * `MyPkg.Sink{T <: Data.StreamType}(schema::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8}, args...; kwargs...)`; given the `schema::Data.Schema` of a `Data.Source`, the type of streaming `T` (`Data.Field` or `Data.Column`), whether the user desires to append the data or not, a possible memory reference `ref` and any `Data.Sink` positional `args...` or keyword arguments `kwargs...`, construct an appropriate instance of `MyPkg.Sink` ready to receive data. The `append` argument allows an already existing sink file/source to "reset" itself if the user does not desire to append.
  * `MyPkg.Sink{T <: Data.StreamType}(sink, schema::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8})`; similar to above, but instead of constructing a new `Sink`, an existing `Sink` is given as a first argument, which may be modified before being returned, ready to receive data according to the `Data.Source` `schema`. Note that a `Data.Sink` should modify itself according to the new `append` and `ref` arguments as well.

Similar to `Data.Source`, a `Data.Sink` also needs to implement it's own `streamto!` method that indicates how it receives data.

A `Data.Sink` supports **field-based** streaming by defining:

  * `Data.streamto!{T}(sink::MyPkg.Sink, ::Type{Data.Field}, val::T, row, col[, schema])`: Given a `row`, `col`, and `val::T` a `Data.Sink` should store the value appropriately. The type of the value retrieved is given by `T`, which may be `Nullable{T}`. Optionally provided is the `schema` (the same `schema` that is passed in the `MyPkg.Sink(schema, ...)` constructors). This argument is passed for efficiency since
  it can be calculated once at the beginning of a `Data.stream!` and used quickly for many calls to `Data.streamto!`. This argument is optional, because a Sink can overload `Data.streamto!` with or without it. Note that it is appropriate for a `Data.Sink` to implement specialized `Data.streamto!` methods that can dispath according to the type `T` of `val::T`, although not strictly required.

A `Data.Sink` supports **column-based** streaming by defining:

    * `Data.streamto!{T}(sink::MyPkg.Sink, ::Type{Data.Column}, column::Type{T}, row, col[, schema])`: Given a column number `col` and column of data `column`, a `Data.Sink` should store it appropriately. The type of the column is given by `T`, which may be a `NullableVector{T}`. Optionally provided is the `schema` (the same `schema` that is passed in the `MyPkg.Sink(schema, ...)` constructors). This argument is passed for efficiency since it can be calculated once at the beginning of a `Data.stream!` and used quickly for many calls to `Data.streamto!`. This argument is optional, because a Sink can overload `Data.streamto!` with or without it.


A `Data.Sink` can optionally define the following if needed:

  * `Data.cleanup!(sink::MyPkg.Sink)`: certain `Data.Sink`, like databases, may need to protect against inconvenient or dangerous "states" if there happens to be an error while streaming. `Data.cleanup!` provides the sink a way to rollback a transaction or other kind of cleanup if an error occurs during streaming
  * `Data.close!(sink::MyPkg.Sink)`: during the `Data.stream!` workflow, a `Data.Sink` should remain "open" to receiving data until a call to `Data.close!`. `Data.close!` is defined to allow a sink to fully commit all streaming results and close/destroy any necessary resources. Note that most convenience functions provided by packages will implicitly call `Data.close!` after streaming has finished from a single `Data.Source` to a single `Data.Sink` (e.g. `CSV.read`, `SQLite.query`, `Feather.read`, `ODBC.query`, etc.).


## `Data.Schema`

```@docs
Data.Schema
```
