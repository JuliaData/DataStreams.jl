__precompile__(true)
module DataStreams

module Data

using Missings, WeakRefStrings

import Core.Compiler: return_type

# Data.Schema
"""
A `Data.Schema` describes a tabular dataset, i.e. a set of named, typed columns with records as rows

`Data.Schema` allow `Data.Source` and `Data.Sink` to talk to each other and prepare to provide/receive data through streaming.
`Data.Schema` provides the following accessible properties:

 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`; `Union{T, Missing}` indicates columns that may contain missing data (`missing` values)
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`; note that # of rows may be `missing`, meaning unknown

`Data.Schema` has the following constructors:

 * `Data.Schema()`: create an "empty" schema with no rows, no columns, and no column names
 * `Data.Schema(types[, header, rows, meta::Dict])`: column element types are provided as a tuple or vector; column names provided as an iterable; # of rows can be an Int or `missing` to indicate unknown # of rows

`Data.Schema` are indexable via column names to get the number of that column in the `Data.Schema`

```julia
julia> sch = Data.Schema([Int], ["column1"], 10)
Data.Schema:
rows: 10	cols: 1
Columns:
 "column1"  Int64

julia> sch["column1"]
1
```

**Developer note**: the full type definition is `Data.Schema{R, T}` where the `R` type parameter will be `true` or `false`, indicating
whether the # of rows are known (i.e not `missing`), respectively. The `T` type parameter is a `Tuple{A, B, ...}` representing the column element types
in the `Data.Schema`. Both of these type parameters provide valuable information that may be useful when constructing `Sink`s or streaming data.
"""
mutable struct Schema{R, T}
    # types::T               # Julia types of columns
    header::Vector{String}   # column names
    rows::Union{Int, Missing}# number of rows in the dataset
    cols::Int                # number of columns in a dataset
    metadata::Dict           # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String, Int} # maps column names as Strings to their index # in `header` and `types`
end

function Schema(types=(), header=["Column$i" for i = 1:length(types)], rows::Union{Integer,Missing}=0, metadata::Dict=Dict())
    !ismissing(rows) && rows < 0 && throw(ArgumentError("Invalid # of rows for Data.Schema; use `missing` to indicate an unknown # of rows"))
    types2 = Tuple(types)
    header2 = String[string(x) for x in header]
    cols = length(header2)
    cols != length(types2) && throw(ArgumentError("length(header): $(length(header2)) must == length(types): $(length(types2))"))
    return Schema{!ismissing(rows), Tuple{types2...}}(header2, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header2)))
end
Schema(types, rows::Union{Integer,Missing}, metadata::Dict=Dict()) = Schema(types, ["Column$i" for i = 1:length(types)], rows, metadata)

header(sch::Schema) = sch.header
types(sch::Schema{R, T}) where {R, T} = Tuple(T.parameters)

function anytypes(sch::Schema{R, T}, weakref) where {R, T}
    types = T.parameters
    if !weakref
        types = map(x->x >: Missing ? ifelse(Missings.T(x) <: WeakRefString, Union{String, Missing}, x) : ifelse(x <: WeakRefString, String, x), types)
    end
    return collect(Any, types)
end

metadata(sch::Schema) = sch.metadata
Base.size(sch::Schema) = (sch.rows, sch.cols)
Base.size(sch::Schema, i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))

setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing
setrows!(source::Array, rows) = nothing
Base.getindex(sch::Schema, col::String) = sch.index[col]

function Base.show(io::IO, schema::Schema)
    println(io, "Data.Schema:")
    println(io, "rows: $(schema.rows)  cols: $(schema.cols)")
    if schema.cols > 0
        println(io, "Columns:")
        Base.print_matrix(io, hcat(schema.header, collect(types(schema))))
    end
end

function transform(sch::Data.Schema{R, T}, transforms::Dict{Int, <:Base.Callable}, weakref) where {R, T}
    types = Data.types(sch)
    transforms2 = ((get(transforms, x, identity) for x = 1:length(types))...,)
    # NOTE: `return_type` is imported, see above
    newtypes = ((return_type(transforms2[x], (types[x],)) for x = 1:length(types))...,)
    if !weakref
        newtypes = map(x->x >: Missing ? ifelse(Missings.T(x) <: WeakRefString, Union{String, Missing}, x) : ifelse(x <: WeakRefString, String, x), newtypes)
    end
    return Schema(newtypes, Data.header(sch), size(sch, 1), sch.metadata), transforms2
end
transform(sch::Data.Schema, transforms::Dict{String, F}, s) where {F<:Base.Callable} =
    transform(sch, Dict{Int, F}(sch[x]=>f for (x, f) in transforms), s)

# Data.StreamTypes
abstract type StreamType end
struct Field  <: StreamType end
struct Row    <: StreamType end
struct Column <: StreamType end

# Data.Source Interface
abstract type Source end

# Required methods
"""
`Data.schema(s::Source) => Data.Schema`

Return the `Data.Schema` of a source, which describes the # of rows & columns, as well as the column types of a dataset.
Some sources like `CSV.Source` or `SQLite.Source` store the `Data.Schema` directly in the type, whereas
others like `DataFrame` compute the schema on the fly.

The `Data.Schema` of a source is used in various ways during the streaming process:

- The # of rows and if they are known are used to generate the inner streaming loop
- The # of columns determine if the innermost streaming loop can be unrolled automatically or not
- The types of the columns are used in loop unrolling to generate efficient and type-stable streaming

See `?Data.Schema` for more details on how to work with the type.
"""
function schema end
"""
`Data.isdone(source, row, col) => Bool`

Checks whether a source can stream additional fields/columns for a desired
`row` and `col` intersection. Used during the streaming process, especially for sources
that have an unknown # of rows, to detect when a source has been exhausted of data.

Data.Source types must at least implement:

`Data.isdone(source::S, row::Int, col::Int)`

If more convenient/performant, they can also implement:

`Data.isdone(source::S, row::Int, col::Int, rows::Union{Int, Missing}, cols::Int)`

where `rows` and `cols` are the size of the `source`'s schema when streaming.

A simple example of how a DataFrame implements this is:
```julia
Data.isdone(df::DataFrame, row, col, rows, cols) = row > rows || col > cols
```
"""
function isdone end
isdone(source, row, col, rows, cols) = isdone(source, row, col)

"""
`Data.streamtype{T<:Data.Source, S<:Data.StreamType}(::Type{T}, ::Type{S}) => Bool`

Indicates whether the source `T` supports streaming of type `S`. To be overloaded by individual sources according to supported `Data.StreamType`s.
This is used in the streaming process to determine the compatability of streaming from a specific source to a specific sink.
It also helps in determining the preferred streaming method, when matched up with the results of `Data.streamtypes(s::Sink)`.

For example, if `MyPkg.Source` supported `Data.Field` streaming, I would define:

```julia
Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Field}) = true
```

and/or for `Data.Column` streaming:

```julia
Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Column}) = true
```
"""
function streamtype end

"""
`Data.reset!(source)`

Resets a source into a state that allows streaming its data again.
For example, for `CSV.Source`, the internal buffer is "seek"ed back to the start position of
the csv data (after the column name headers). For `SQLite.Source`, the source SQL query is re-executed.
"""
function reset! end

"""
Data.Source types must implement one of the following:

`Data.streamfrom(source, ::Type{Data.Field}, ::Type{T}, row::Int, col::Int) where {T}`

`Data.streamfrom(source, ::Type{Data.Column}, ::Type{T}, col::Int) where {T}`

Performs the actually streaming of data "out" of a data source. For `Data.Field` streaming, the single field value of type `T`
at the intersection of `row` and `col` is returned. For `Data.Column` streaming, the column # `col` with element type `T` is returned.

For `Data.Column`, a source can also implement:
```julia
Data.streamfrom(source, ::Type{Data.Field}, ::Type{T}, row::Int, col::Int) where {T}
```
where `row` indicates the # of rows that have already been streamed from the source.
"""
function streamfrom end
Data.streamfrom(source, ::Type{Data.Column}, T, row, col) = Data.streamfrom(source, Data.Column, T, col)
Data.streamfrom(source, ::Type{Data.Column}, T, r::AbstractRange, col) = Data.streamfrom(source, Data.Column, T, first(r), col)[r]

# Generic fallbacks
Data.streamtype(source, ::Type{Data.Row}) = Data.streamtype(source, Data.Field)
Data.streamtype(source, ::Type{<:StreamType}) = false
Data.reset!(source) = nothing

struct RandomAccess end
struct Sequential end

"""
`Data.accesspattern(source) => Data.RandomAccess | Data.Sequential`

returns the data access pattern for a Data.Source.

`RandomAccess` indicates that a source supports streaming data (via calls to `Data.streamfrom`) with arbitrary row/column values in any particular order.

`Sequential` indicates that the source only supports streaming data sequentially, starting w/ row 1, then accessing each column from 1:N, then row 2, and each column from 1:N again, etc.

For example, a `DataFrame` holds all data in-memory, and thus supports easy random access in any order.
A `CSV.Source` however, is streaming the contents of a file, where rows must be read sequentially, and each column sequentially within each rows.

By default, sources are assumed to have a `Sequential` access pattern.
"""
function accesspattern end

accesspattern(x) = Sequential()

const EMPTY_REFERENCE = UInt8[]
"""
Data.Source types can optionally implement

`Data.reference(x::Source) => Vector{UInt8}`

where the type retruns a `Vector{UInt8}` that represents a memory block that should be kept in reference for WeakRefStringArrays.

In many streaming situations, the minimizing of data copying/movement is ideal. Some sources can provide in-memory access to their data
in the form of a `Vector{UInt8}`, i.e. a single byte vector, that sinks can "point to" when streaming, instead of needing to always
copy all the data. In particular, the `WeakRefStrings` package provides utilities for creating "string types" that don't actually hold their
own data, but instead just "point" to data that lives elsewhere. As Strings can be some of the most expensive data structures to copy
and move around, this provides excellent performance gains in some cases when the sink is able to leverage this alternative structure.
"""
function reference end
reference(x) = EMPTY_REFERENCE

# Data.Sink Interface
"""
Represents a type that can have data streamed to it from `Data.Source`s.

To satisfy the `Data.Sink` interface, it must provide two constructors with the following signatures:

```
[Sink](sch::Data.Schema, S::Type{StreamType}, append::Bool, args...; reference::Vector{UInt8}=UInt8[], kwargs...) => [Sink]
[Sink](sink, sch::Data.Schema, S::Type{StreamType}, append::Bool; reference::Vector{UInt8}=UInt8[]) => [Sink]
```

Let's break these down, piece by piece:

- `[Sink]`: this is your sink type, i.e. `CSV.Sink`, `DataFrame`, etc. You're defining a constructor for your sink type.
- `sch::Data.Schema`: in the streaming process, the schema of a `Data.Source` is provided to the sink in order to allow the sink to "initialize" properly in order to receive data according to the format in `sch`. This might mean pre-allocating space according to the # of rows/columns in the source, managing the sink's own schema to match `sch`, etc.
- `S::Type{StreamType}`: `S` represents the type of streaming that will occur from the `Data.Source`, either `Data.Field` or `Data.Column`
- `append::Bool`: a boolean flag indicating whether the data should be appended to a sink's existing data store, or, if `false`, if the sink's data should be fully replaced by the incoming `Data.Source`'s data
- `args...`: In the 1st constructor form, `args...` represents a catchall for any additional arguments your sink may need to construct. For example, `SQLite.jl` defines `Sink(sch, S, append, db, table_name)`, meaning that the `db` and `table_name` are additional required arguments in order to properly create an `SQLite.Sink`.
- `reference::Vector{UInt8}`: if your sink defined `Data.weakrefstrings(sink::MySink) = true`, then it also needs to be able to accept the `reference` keyword argument, where a source's memory block will be passed, to be held onto appropriately by the sink when streaming WeakRefStrings. If a sink does not support streaming WeakRefStrings (the default), the sink constructor doesn't need to support any keyword arguments.
- `kwargs...`: Similar to `args...`, `kwargs...` is a catchall for any additional keyword arguments you'd like to expose for your sink constructor, typically matching supported keyword arguments that are provided through the normal sink constructor
- `sink`: in the 2nd form, an already-constructed sink is passed in as the 1st argument. This allows efficient sink re-use. This constructor needs to ensure the existing sink is modified (enlarged, shrunk, schema changes, etc) to be ready to accept the incoming source data as described by `sch`.

Now let's look at an example implementation from CSV.jl:

```julia
function CSV.Sink(fullpath::AbstractString; append::Bool=false, headers::Bool=true, colnames::Vector{String}=String[], kwargs...)
    io = IOBuffer()
    options = CSV.Options(kwargs...)
    !append && header && !isempty(colnames) && writeheaders(io, colnames, options)
    return CSV.Sink(options, io, fullpath, position(io), !append && header && !isempty(colnames), colnames, length(colnames), append)
end

function CSV.Sink(sch::Data.Schema, T, append, file::AbstractString; reference::Vector{UInt8}=UInt8[], kwargs...)
    sink = CSV.Sink(file; append=append, colnames=Data.header(sch), kwargs...)
    return sink
end

function CSV.Sink(sink, sch::Data.Schema, T, append; reference::Vector{UInt8}=UInt8[])
    sink.append = append
    sink.cols = size(sch, 2)
    !sink.header && !append && writeheaders(sink.io, Data.header(sch), sink.options, sink.quotefields)
    return sink
end
```

In this case, CSV.jl defined an initial constructor that just takes the filename with a few keyword arguments.
The two required Data.Sink constructors are then defined. The first constructs a new Sink, requiring a `file::AbstractString` argument.
We also see that `CSV.Sink` supports WeakRefString streaming by accepting a `reference` keyword argument (which is trivially implemented for CSV, since all data is simply written out to disk as text).

For the 2nd (last) constructor in the definitions above, we see the case where an existing `sink` is passed to `CSV.Sink`.
The sink updates a few of its fields (`sink.append = append`), and some logic is computed to determine if
the column headers should be written.
"""
abstract type Sink end

"""
`Data.streamtypes(::Type{[Sink]}) => Vector{StreamType}`

Returns a vector of `Data.StreamType`s that the sink is able to receive; the order of elements indicates the sink's streaming preference

For example, if my sink only supports `Data.Field` streaming, I would simply define:
```julia
Data.streamtypes(::Type{MyPkg.Sink}) = [Data.Field]
```

If, on the other hand, my sink also supported `Data.Column` streaming, and `Data.Column` streaming happend to be more efficient, I could define:
```julia
Data.streamtypes(::Type{MyPkg.Sink}) = [Data.Column, Data.Field] # put Data.Column first to indicate preference
```

A third option is a sink that operates on entire rows at a time, in which case I could define:
```julia
Data.streamtypes(::Type{MyPkg.Sink}) = [Data.Row]
```
The subsequent `Data.streamto!` method would then require the signature `Data.streamto!(sink::MyPkg.Sink, ::Type{Data.Row}, vals::NamedTuple, row, col, knownrows`
"""
function streamtypes end

"""
`Data.streamto!(sink, S::Type{StreamType}, val, row, col)`

`Data.streamto!(sink, S::Type{StreamType}, val, row, col, knownrows)`

Streams data to a sink. `S` is the type of streaming (`Data.Field`, `Data.Row`, or `Data.Column`). `val` is the value or values (single field, row as a NamedTuple, or column, respectively)
to be streamed to the sink. `row` and `col` indicate where the data should be streamed/stored.

A sink may optionally define the method that also accepts the `knownrows` argument, which will be `Val{true}` or `Val{false}`,
indicating whether the source streaming has a known # of rows or not. This can be useful for sinks that
may know how to pre-allocate space in the cases where the source can tell the # of rows, or in the case
of unknown # of rows, may need to stream the data in differently.
"""
function streamto! end
Data.streamto!(sink, S, val, row, col, knownrows) = Data.streamto!(sink, S, val, row, col)
Data.streamto!(sink, S, val, row, col) = Data.streamto!(sink, S, val, col)

# Optional methods
"""
`Data.cleanup!(sink)`

Sometimes errors occur during the streaming of data from source to sink. Some sinks may be left in
an undesired state if an error were to occur mid-streaming. `Data.cleanup!` allows a sink to "clean up"
any necessary resources in the case of a streaming error. `SQLite.jl`, for example, defines:
```julia
function Data.cleanup!(sink::SQLite.Sink)
    rollback(sink.db, sink.transaction)
    return
end
```
Since a database transaction is initiated at the start of streaming, it must be rolled back in the case of streaming error.

The default definition is: `Data.cleanup!(sink) = nothing`
"""
function cleanup! end

"""
`Data.close!(sink) => sink`

A function to "close" a sink to streaming. Some sinks require a definitive time where data can be "committed",
`Data.close!` allows a sink to perform any necessary resource management or commits to ensure all data that has been
streamed is stored appropriately. For example, the `SQLite` package defines:
```julia
function Data.close!(sink::SQLite.Sink)
    commit(sink.db, sink.transaction)
    return sink
end
```
Which commits a database transaction that was started when the sink was initially "opened".
"""
function close! end

# Generic fallbacks
cleanup!(sink) = nothing
close!(sink) = sink

"""
`Data.weakrefstrings(sink) => Bool`

If a sink is able to appropriately handle `WeakRefString` objects, it can define:
```julia
Data.weakrefstrings(::Type{[Sink]}) = true
```
to indicate that a source may stream those kinds of values to it. By default, sinks do
not support WeakRefString streaming. Supporting WeakRefStrings corresponds to accepting the
`reference` keyword argument in the required sink constructor method, see `?Data.Sink`.
"""
function weakrefstrings end
weakrefstrings(x) = false

# Data.stream!
"""
`Data.stream!(source, sink; append::Bool=false, transforms=Dict())`

`Data.stream!(source, ::Type{Sink}, args...; append::Bool=false, transforms=Dict(), kwargs...)`

Stream data from source to sink. The 1st definition assumes already constructed source & sink and takes two optional keyword arguments:

- `append::Bool=false`: whether the data from `source` should be appended to `sink`
- `transforms::Dict`: A dict with mappings between column # or name (Int or String) to a "transform" function. For `Data.Field` streaming, the transform function should be of the form `f(x::T) => y::S`, i.e. takes a single input of type `T` and returns a single value of type `S`. For `Data.Column` streaming, it should be of the form `f(x::AbstractVector{T}) => y::AbstractVector{S}`, i.e. take an AbstractVector with eltype `T` and return another AbstractVector with eltype `S`.

For the 2nd definition, the Sink type itself is passed as the 2nd argument (`::Type{Sink}`) and is constructed "on-the-fly", being passed `args...` and `kwargs...` like `Sink(args...; kwargs...)`.

While users are free to call `Data.stream!` themselves, oftentimes, packages want to provide even higher-level convenience functions.

An example of of these higher-level convenience functions are from CSV.jl:

```julia
function CSV.read(fullpath::Union{AbstractString,IO}, sink::Type=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
    source = CSV.Source(fullpath; kwargs...)
    sink = Data.stream!(source, sink, args...; append=append, transforms=transforms, kwargs...)
    return Data.close!(sink)
end

function CSV.read(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...) where T
    source = CSV.Source(fullpath; kwargs...)
    sink = Data.stream!(source, sink; append=append, transforms=transforms)
    return Data.close!(sink)
end
```
In this example, CSV.jl defines it's own high-level function for reading from a `CSV.Source`. In these examples, a `CSV.Source` is constructed using the `fullpath` argument, along w/ any extra `kwargs...`.
The sink can be provided as a type with `args...` and `kwargs...` that will be passed to its DataStreams constructor, like `Sink(sch, streamtype, append, args...; kwargs...)`; otherwise, an already-constructed
Sink can be provided directly (2nd example).

Once the `source` is constructed, the data is streamed via the call to `Data.stream(source, sink; append=append, transforms=transforms)`, with the sink being returned.

And finally, to "finish" the streaming process, `Data.close!(sink)` is closed, which returns the finalized sink. Note that `Data.stream!(source, sink)` could be called multiple times with different sources and the same sink,
most likely with `append=true` being passed, to enable the accumulation of several sources into a single sink. A single `Data.close!(sink)` method should be called to officially close or commit the final sink.

Two "builtin" Source/Sink types that are included with the DataStreams package are the `Data.Table` and `Data.RowTable` types. `Data.Table` is a NamedTuple of AbstractVectors, with column names as NamedTuple fieldnames.
This type supports both `Data.Field` and `Data.Column` streaming. `Data.RowTable` is just a Vector of NamedTuples, and as such, only supports `Data.Field` streaming.

In addition, any `Data.Source` can be iterated via the `Data.rows(source)` function, which returns a NamedTuple-iterator over the rows of a source.
"""
function stream! end

# skipfield! and skiprow! only apply to Data.Field/Data.Row streaming
skipfield!(source, S, T, row, col) = Data.accesspattern(source) == Data.RandomAccess() ? nothing : Data.streamfrom(source, S, T, row, col)
function skiprow!(source, S, row, col)
    Data.accesspattern(source) == Data.RandomAccess() && return
    sch = Data.schema(source)
    cols = size(sch, 2)
    types = Data.types(sch)
    for i = col:cols
        Data.streamfrom(source, S, types[i], row, i)
    end
    return
end
function skiprows!(source, S, from, to)
    Data.accesspattern(source) == Data.RandomAccess() && return
    sch = Data.schema(source)
    cols = size(sch, 2)
    types = Data.types(sch)
    for row = from:to
        for col = 1:cols
            Data.streamfrom(source, S, types[col], row, col)
        end
    end
end

datatype(T) = Core.eval(parentmodule(Base.unwrap_unionall(T)), nameof(T))

include("namedtuples.jl")
include("query.jl")

end # module Data

using .Data
export Data

end # module DataStreams
