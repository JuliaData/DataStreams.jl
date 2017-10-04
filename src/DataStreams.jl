__precompile__(true)
module DataStreams

export Data

module Data

using Nulls, WeakRefStrings

struct NullException <: Exception
    msg::String
end

# Data.Schema
"""
A `Data.Schema` describes a tabular dataset, i.e. a set of named, typed columns with records as rows

`Data.Schema` allow `Data.Source` and `Data.Sink` to talk to each other and prepare to provide/receive data through streaming.
`Data.Schema` provides the following accessible properties:

 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`; `Nullable{T}` indicates columns that may contain missing data (null values)
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`; note that # of rows may be `null`, meaning unknown

`Data.Schema` has the following constructors:

 * `Data.Schema()`: create an "emtpy" schema with no rows, no columns, and no column names
 * `Data.Schema(types[, header, rows, meta::Dict])`: column element types are provided as a tuple or vector; column names provided as an iterable; # of rows can be an Int or `null` to indicate unknown # of rows

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
whether the # of rows are known (i.e not `null`), respectively. The `T` type parameter is a `Tuple{A, B, ...}` representing the column element types
in the `Data.Schema`. Both of these type parameters provide valuable information that may be useful when constructing `Sink`s or streaming data.
"""
mutable struct Schema{R, T}
    # types::T               # Julia types of columns
    header::Vector{String}   # column names
    rows::(Union{Int,Null})  # number of rows in the dataset
    cols::Int                # number of columns in a dataset
    metadata::Dict           # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String, Int} # maps column names as Strings to their index # in `header` and `types`
end

function Schema(types=(), header=["Column$i" for i = 1:length(types)], rows::(Union{Integer,Null})=0, metadata::Dict=Dict())
    !isnull(rows) && rows < 0 && throw(ArgumentError("Invalid # of rows for Data.Schema; use `null` to indicate an unknown # of rows"))
    types2 = Tuple(types)
    header2 = String[string(x) for x in header]
    cols = length(header2)
    cols != length(types2) && throw(ArgumentError("length(header): $(length(header2)) must == length(types): $(length(types2))"))
    return Schema{!isnull(rows), Tuple{types2...}}(header2, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header2)))
end
Schema(types, rows::Union{Integer,Null}, metadata::Dict=Dict()) = Schema(types, ["Column$i" for i = 1:length(types)], rows, metadata)

header(sch::Schema) = sch.header
types(sch::Schema{R, T}) where {R, T} = Tuple(T.parameters)
metadata(sch::Schema) = sch.metadata
Base.size(sch::Schema) = (sch.rows, sch.cols)
Base.size(sch::Schema, i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))

setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing
Base.getindex(sch::Schema, col::String) = sch.index[col]

function Base.show(io::IO, schema::Schema)
    println(io, "Data.Schema:")
    println(io, "rows: $(schema.rows)  cols: $(schema.cols)")
    if schema.cols > 0
        println(io, "Columns:")
        Base.print_matrix(io, hcat(schema.header, collect(types(schema))))
    end
end

function transform(sch::Data.Schema{R, T}, transforms::Dict{Int, <:Function}, weakref) where {R, T}
    types = Data.types(sch)
    transforms2 = ((get(transforms, x, identity) for x = 1:length(types))...)
    newtypes = ((Core.Inference.return_type(transforms2[x], (types[x],)) for x = 1:length(types))...)
    if !weakref
        newtypes = map(x->x >: Null ? ifelse(Nulls.T(x) <: WeakRefString, Union{String, Null}, x) : ifelse(x <: WeakRefString, String, x), newtypes)
    end
    return Schema(newtypes, Data.header(sch), size(sch, 1), sch.metadata), transforms2
end
transform(sch::Data.Schema, transforms::Dict{String, <:Function}, s) = transform(sch, Dict{Int, Function}(sch[x]=>f for (x, f) in transforms), s)

# Data.StreamTypes
abstract type StreamType end
struct Field <: StreamType end
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

`Data.isdone(source::S, row::Int, col::Int, rows::Union{Int, Null}, cols::Int)`

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

# Generic fallbacks
Data.streamtype(source, ::Type{<:StreamType}) = false
Data.reset!(source) = nothing

struct RandomAccess end
struct Sequential end

"""
`Data.accesspatern(source) => Data.RandomAccess | Data.Sequential`

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
"""
function streamtypes end

"""
`Data.streamto!(sink, S::Type{StreamType}, val, row, col)`

`Data.streamto!(sink, S::Type{StreamType}, val, row, col, knownrows)`

Streams data to a sink. `S` is the type of streaming (`Data.Field` or `Data.Column`). `val` is the value (single field or column)
to be streamed to the sink. `row` and `col` indicate where the data should be streamed/stored.

A sink may optionally define the method that also accepts the `knownrows` argument, which will be `true` or `false`,
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

function CSV.read{T}(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)
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
"""
function stream! end

datatype(T) = eval(Base.datatype_module(Base.unwrap_unionall(T)), Base.datatype_name(T))

# generic public definitions
const TRUE = x->true
# the 2 methods below are safe and expected to be called from higher-level package convenience functions (e.g. CSV.read)
function Data.stream!(source::So, ::Type{Si}, args...;
                        append::Bool=false,
                        transforms::Dict=Dict{Int, Function}(),
                        filter::Function=TRUE,
                        columns::Vector=[],
                        kwargs...) where {So, Si}
    S = datatype(Si)
    sinkstreamtypes = Data.streamtypes(S)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(datatype(So), sinkstreamtype)
            source_schema = Data.schema(source)
            wk = weakrefstrings(S)
            sink_schema, transforms2 = Data.transform(source_schema, transforms, wk)
            if wk
                sink = S(sink_schema, sinkstreamtype, append, args...; reference=Data.reference(source), kwargs...)
            else
                sink = S(sink_schema, sinkstreamtype, append, args...; kwargs...)
            end
            sourcerows = size(source_schema, 1)
            sinkrows = size(sink_schema, 1)
            sinkrowoffset = ifelse(append, ifelse(isnull(sourcerows), sinkrows, max(0, sinkrows - sourcerows)), 0)
            return Data.stream!(source, sinkstreamtype, sink, source_schema, sinkrowoffset, transforms2, filter, columns)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

function Data.stream!(source::So, sink::Si;
                        append::Bool=false,
                        transforms::Dict=Dict{Int, Function}(),
                        filter::Function=TRUE,
                        columns::Vector=[]) where {So, Si}
    S = datatype(Si)
    sinkstreamtypes = Data.streamtypes(S)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(datatype(So), sinkstreamtype)
            source_schema = Data.schema(source)
            wk = weakrefstrings(S)
            sink_schema, transforms2 = transform(source_schema, transforms, wk)
            if wk
                sink = S(sink, sink_schema, sinkstreamtype, append; reference=Data.reference(source))
            else
                sink = S(sink, sink_schema, sinkstreamtype, append)
            end
            sourcerows = size(source_schema, 1)
            sinkrows = size(sink_schema, 1)
            sinkrowoffset = ifelse(append, ifelse(isnull(sourcerows), sinkrows, max(0, sinkrows - sourcerows)), 0)
            return Data.stream!(source, sinkstreamtype, sink, source_schema, sinkrowoffset, transforms2, filter, columns)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

# column filtering
 # Data.transforms needs to produce sink schema w/ correct #/types of columns
 # if RandomAccess
   # only generate @nexprs for selected column #s, need to pass in column offset or something to map between source co
 # else
  # generate normal # of @nexprs, but only need to include Data.streamto! for included columns

# row filtering
 # needs to update source schema to be unknown # of rows? or maybe that gets set earlier?
 # where func: (tuple) -> Bool or (tuple, row) -> Bool
 # probably do @nexprs for streamfroms first, then apply where func
 # if where = true, execute @nexprs streamto!, else continue

function inner_loop(::Type{Val{N}}, ::Type{S}, ::Type{Val{homogeneous}}, ::Type{T}, knownrows::Type{Val{R}}) where {N, S <: StreamType, homogeneous, T, R}
    if N < 500
        # println("generating inner_loop w/ @nexprs...")
        incr = S == Data.Column ? :(cur_row = length($(Symbol(string("val_", N))))) : :(nothing)
        loop = quote
            Base.@nexprs $N col->begin
                val_col = Data.streamfrom(source, $S, sourcetypes[col], row, col)
                # hack to improve codegen due to inability of inference to inline Union{T, Null} val_col here
                if val_col isa Null
                    Data.streamto!(sink, $S, transforms[col](val_col), sinkrowoffset + row, col, $knownrows)
                else
                    Data.streamto!(sink, $S, transforms[col](val_col), sinkrowoffset + row, col, $knownrows)
                end
            end
            $incr
        end
    elseif homogeneous
        # println("generating inner_loop w/ homogeneous types...")
        loop = quote
            for col = 1:$N
                val = Data.streamfrom(source, $S, $T, row, col)
                if val isa Null
                    Data.streamto!(sink, $S, transforms[col](val), sinkrowoffset + row, col, $knownrows)
                else
                    Data.streamto!(sink, $S, transforms[col](val), sinkrowoffset + row, col, $knownrows)
                end
                $(S == Data.Column && :(cur_row = length(val)))
            end
        end
    else
        # println("generating inner_loop w/ > 500 columns...")
        loop = quote
            for col = 1:cols
                @inbounds cur_row = Data.streamto!(sink, $S, source, sourcetypes[col], row, sinkrowoffset, col, transforms[col], $knownrows)
            end
        end
    end
    # println(macroexpand(loop))
    return loop
end

@inline function streamto!(sink, ::Type{S}, source, ::Type{T}, row, sinkrowoffset, col::Int, f::Function, knownrows) where {S, T}
    val = Data.streamfrom(source, S, T, row, col)
    if val isa Null
        Data.streamto!(sink, S, f(val), sinkrowoffset + row, col, knownrows)
    else
        Data.streamto!(sink, S, f(val), sinkrowoffset + row, col, knownrows)
    end
    return length(val)
end

function generate_loop(::Type{Val{knownrows}}, ::Type{S}, inner_loop) where {knownrows, S <: StreamType}
    if knownrows && S == Data.Field
        # println("generating loop w/ known rows...")
        loop = quote
            for row = 1:rows
                $inner_loop
            end
        end
    else
        # println("generating loop w/ unknown rows...")
        loop = quote
            row = cur_row = 1
            while true
                $inner_loop
                row += cur_row # will be 1 for Data.Field, length(val) for Data.Column
                Data.isdone(source, row, cols, rows, cols) && break
            end
            Data.setrows!(source, row)
        end
    end
    # println(macroexpand(loop))
    return loop
end

@generated function Data.stream!(source::So, ::Type{S}, sink::Si,
                        source_schema::Schema{R, T1}, sinkrowoffset,
                        transforms, filter, columns) where {So, S <: StreamType, Si, R, T1}
    types = T1.parameters
    sourcetypes = Tuple(types)
    homogeneous = Val{all(i -> (types[1] === i), types)}
    T = isempty(types) ? Any : types[1]
    N = Val{length(types)}
    knownrows = R ? Val{true} : Val{false}
    RR = R ? Int : Null
    r = quote
        rows, cols = size(source_schema)::Tuple{$RR, Int}
        Data.isdone(source, 1, 1, rows, cols) && return sink
        sourcetypes = $sourcetypes
        N = $N
        try
            $(generate_loop(knownrows, S, inner_loop(N, S, homogeneous, T, knownrows)))
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
    # println(r)
    return r
end

if isdefined(Core, :NamedTuple)

# Basically, a NamedTuple with any # of AbstractVector elements, accessed by column name
const Table = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}

# NamedTuple Data.Source implementation
# compute Data.Schema on the fly
function Data.schema(df::NamedTuple{names, T}) where {names, T}
    return Data.Schema(Type[eltype(A) for A in T.parameters],
                        collect(map(string, names)), length(df) == 0 ? 0 : length(getfield(df, 1)))
end

Data.isdone(source::Table, row, col, rows, cols) = row > rows || col > cols
function Data.isdone(source::Table, row, col)
    cols = length(source)
    return Data.isdone(source, row, col, cols == 0 ? 0 : length(getfield(source, 1)), cols)
end

# We support both kinds of streaming
Data.streamtype(::Type{NamedTuple}, ::Type{Data.Column}) = true
Data.streamtype(::Type{NamedTuple}, ::Type{Data.Field}) = true

# Data.streamfrom is pretty simple, just return the cell or column
@inline Data.streamfrom(source::Table, ::Type{Data.Column}, ::Type{T}, row, col) where {T} = source[col]
@inline Data.streamfrom(source::Table, ::Type{Data.Field}, ::Type{T}, row, col) where {T} = source[col][row]

# NamedTuple Data.Sink implementation
# we support both kinds of streaming to our type
Data.streamtypes(::Type{NamedTuple}) = [Data.Column, Data.Field]
# we support streaming WeakRefStrings
Data.weakrefstrings(::Type{NamedTuple}) = true

# convenience methods for "allocating" a single column for streaming
allocate(::Type{T}, rows, ref) where {T} = Vector{T}(rows)
# allocate(::Type{T}, rows, ref) where {T <: Union{CategoricalValue, Null}} =
#     CategoricalArray{CategoricalArrays.unwrap_catvalue_type(T)}(rows)
# special case for WeakRefStrings
allocate(::Type{T}, rows, ref) where {T <: Union{WeakRefString,Null}} = WeakRefStringArray(ref, T, rows)

# NamedTuple doesn't allow duplicate names, so make sure there are no duplicates in our column names
function makeunique(names::Vector{String})
    nms = [Symbol(nm) for nm in names]
    seen = Set{Symbol}()
    for (i, x) in enumerate(nms)
        x in seen ? setindex!(nms, Symbol("$(x)_$i"), i) : push!(seen, x)
    end
    return (nms...)
end

# Construct or modify a NamedTuple to be ready to stream data from a source with a schema of `sch`
# We support WeakRefString streaming, so we include the `reference` keyword
function NamedTuple(sch::Data.Schema{R}, ::Type{S}=Data.Field,
                    append::Bool=false, args...; reference::Vector{UInt8}=UInt8[]) where {R, S <: StreamType}
    types = Data.types(sch)
    # check if we're dealing with an existing NamedTuple sink or not
    if !isempty(args) && args[1] isa Table && types == Data.types(Data.schema(args[1]))
        # passing in an existing NamedTuple Sink w/ same types as source (as indicated by `sch`)
        sink = args[1]
        sinkrows = size(Data.schema(sink), 1)
        if append && (S == Data.Column || !R) # are we appending and either column-streaming or there are an unknown # of rows
            sch.rows = sinkrows
            # dont' need to do anything because:
              # for Data.Column, we just append columns anyway (see Data.streamto! below)
              # for Data.Field, the # of rows in the source are unknown (isnull(rows)), so we'll just push! in streamto!
        else
            # need to adjust the existing sink
            # similar to above, for Data.Column or unknown # of rows for Data.Field, we'll append!/push!, so we empty! the columns
            # if appending, we want to grow our columns to be able to include every row in source (sinkrows + sch.rows)
            # if not appending, we're just "re-using" a sink, so we just resize it to the # of rows in the source
            newsize = ifelse(S == Data.Column || !R, 0, ifelse(append, sinkrows + sch.rows, sch.rows))
            foreach(col->resize!(col, newsize), sink)
            sch.rows = newsize
        end
        # take care of a possible reference from source by letting WeakRefStringArrays hold on to them
        if !isempty(reference)
            foreach(col-> col isa WeakRefStringArray && push!(col.data, reference), sink)
        end
    else
        # allocating a fresh NamedTuple Sink; append is irrelevant
        # for Data.Column or unknown # of rows in Data.Field, we only ever append!, so just allocate empty columns
        rows = ifelse(S == Data.Column, 0, ifelse(!R, 0, sch.rows))
        names = makeunique(Data.header(sch))
        sink = Base.namedtuple(NamedTuple{names}, (allocate(types[i], rows, reference) for i = 1:length(types))...)
        sch.rows = rows
    end
    return sink
end

# Constructor that takes an existing NamedTuple sink, just pass it to our mega-constructor above
function NamedTuple(sink::Table, sch::Data.Schema, ::Type{S}, append::Bool; reference::Vector{UInt8}=UInt8[]) where {S}
    return NamedTuple(sch, S, append, sink; reference=reference)
end

# Data.streamto! is easy-peasy, if there are known # of rows from source, we pre-allocated
# so we can just set the value; otherwise (didn't pre-allocate), we push!/append! the values
@inline Data.streamto!(sink::Table, ::Type{Data.Field}, val, row, col::Int) =
    (A = getfield(sink, col); row > length(A) ? push!(A, val) : setindex!(A, val, row))
@inline Data.streamto!(sink::Table, ::Type{Data.Field}, val, row, col::Int, ::Type{Val{false}}) =
    push!(getfield(sink, col), val)
@inline Data.streamto!(sink::Table, ::Type{Data.Field}, val, row, col::Int, ::Type{Val{true}}) =
    getfield(sink, col)[row] = val
@inline Data.streamto!(sink::Table, ::Type{Data.Column}, column, row, col::Int, knownrows) =
    append!(getfield(sink, col), column)

end # if isdefined(Core, :NamedTuple)
end # module Data

end # module DataStreams
