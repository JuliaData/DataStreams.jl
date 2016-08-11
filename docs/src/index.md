# DataStreams.jl

The `DataStreams.jl` package aims to define a generic and performant framework for the transfer of "table-like" data. (i.e. data that can, in some sense, be described by rows and columns).

The framework achieves this by defining a system of `Data.Source` types and methods to describe how they "provide" data; as well as `Data.Sink` types and methods around how they "receive" data. This allows `Data.Source`s and `Data.Sink`s to implement their interfaces separately, without needing to be aware of each other. The end result is an ecosystem of packages that "automatically" talk with each other, with adding an additional package requiring no additional machinery in an existing packages.

## `Data.Source` Interface

The `Data.Source` interface requires the following definitions, where `MyPkg` would represent a package wishing to implement the framework:

  * `Data.schema(::MyPkg.Source) => Data.Schema`; get the `Data.Schema` of a `Data.Source`. Typically the `Source` type will store the `Data.Schema` directly, but this isn't strictly required. See `?Data.Schema` or docs below for more information on `Data.Schema`
  * `Data.isdone(::MyPkg.Source, row, col) => Bool`; indicates whether the `Data.Source` will be able to provide data, given a `row` and `col`.

Optional definition:

  * `Data.reference(::MyPkg.Source) => Vector{UInt8}`; Sometimes, a `Source` needs the `Sink` to keep a reference to memory to keep a data structure valid. A `Source` can implement this method to return a `Vector{UInt8}` that the `Sink` will need to handle appropriately.

A `Data.Source` also needs to "register" the type (or types) of streaming it supports. Currently defined streaming types in the DataStreams framework include:

  * `Data.Field`: a field is the intersection of a specific row and column; this type of streaming will traverse the "table" structure by row, accessing each column on each row
  * `Data.Column`: this type of streaming will provide entire columns at a time

A `Data.Source` "registers" to support field-based streaming by defining the following:

  * `Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Field}) = true`; declares that `MyPkg.Source` supports field-based streaming
  * `Data.getfield{T}(::MyPkg.Source, ::Type{T}, row, col) => Nullable{T}`; returns a value of type `Nullable{T}` given a specific `row` and `col` from `MyPkg.Source`

And for column-based streaming:

  * `Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Column}) = true`  
  * `Data.getcolumn{T}(::Data.Source, ::Type{T}, col) => AbstractVector{T}`

## `Data.Sink` Interface

Similar to a `Data.Source`, a `Data.Sink` needs to "register" the types of streaming it supports, it does so through the following definition:

  * `Data.streamtypes(::Type{MyPkg.Sink}) = [Data.Field[, Data.Column]]`; "registers" the streaming preferences for `MyPkg.Sink`. A `Sink` type should list the stream type or types it supports. If the `Sink` supports streaming of multiple types, it should list them in order of preference (i.e. the more natural or performant type first).

A `Data.Sink` should also implement specific forms of constructors that allow convenience in many higher-level streaming functions:

  * `MyPkg.Sink{T <: Data.StreamType}(source, ::Type{T}, append::Bool, args...)`; given an instance of a `Data.Source`, the type of streaming `T`, whether the user desires to append `source` or not, and any necessary `args...`, construct an appropriate instance of `MyPkg.Sink` ready to receive data from `source`.
  * `MyPkg.Sink{T <: Data.StreamType}(sink, source, ::Type{T}, append::Bool)`; similar to above, but instead of constructing a new `Sink`, an existing `Sink` is given as a first argument, which may be modified before being returned, ready to receive data from `source`.

And finally, a `Data.Sink` needs to implement the meat of the framework, the actual streaming method. For a `Sink` supporting field-based streaming, the following method should be defined:

  * `Data.stream!(source, ::Type{Data.Field}, sink::MyPkg.Sink, append::Bool)`; given a generic `Data.Source`, continue streaming data until `Data.isdone(source, row, col) == true`. The streaming method should usually check `Data.isdone(source, 1, 1) && return sink` before starting the actual streaming to account for a potentially empty `Data.Source`.

And for column-based streaming:

  * `Data.stream!(source, ::Type{Data.Column}, sink::MyPkg.Sink, append::Bool)`
