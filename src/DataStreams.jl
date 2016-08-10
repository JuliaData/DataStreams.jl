VERSION >= v"0.4.0-dev+6521" && __precompile__(true)
"""
The `DataStreams.jl` package defines a data processing framework based on Sources, Sinks, and the `Data.stream!` function.

`DataStreams` defines the common infrastructure leveraged by individual packages to create systems of various
data sources and sinks that talk to each other in a unified, consistent way.

The workflow enabled by the `DataStreams` framework involves:

 * constructing new `Source` types to allow streaming data from files, databases, etc.
 * `Data.stream!` those datasets to newly created or existing `Sink` types
 * convert `Sink` types that have received data into new `Source` types
 * continue to `Data.stream!` from `Source`s to `Sink`s

The typical approach for a new package to "satisfy" the DataStreams interface is to:

 * Define a `Source` type that wraps an "true data source" (i.e. a file, database table/query, etc.) and fulfills the `Source` interface (see `?Data.Source`)
 * Define a `Sink` type that can create or write data to an "true data source" and fulfills the `Sink` interface (see `?Data.Sink`)
 * Define appropriate `Data.stream!(::Source, ::Sink)` methods as needed between various combinations of Sources and Sinks;
   i.e. define `Data.stream!(::NewPackage.Source, ::CSV.Sink)` and `Data.stream!(::CSV.Source, ::NewPackage.Sink)`
"""
module DataStreams

export Data, DataFrame

module Data

if !isdefined(Core, :String)
    typealias String UTF8String
end

"""
A `Data.Source` type holds data that can be read/queried/parsed/viewed/streamed; i.e. a "true data source"
To clarify, there are two distinct types of "source":

  1) the "true data source", which would be the file, database, API, structure, etc; i.e. the actual data
  2) the `Data.Source` julia object that wraps the "true source" and provides the `DataStreams` interface

`Source` types have two different types of constructors:

  1) "independent constructors" that wrap "true data sources"
  2) "sink constructors" where a `Data.Sink` object that has received data is turned into a new `Source` (useful for chaining data processing tasks)

`Source`s also have a, currently implicit, notion of state:

  * `BEGINNING`: a `Source` is in this state immediately after being constructed and is ready to be used; i.e. ready to read/parse/query/stream data from it
  * `READING`: the ingestion of data from this `Source` has started and has not finished yet
  * `DONE`: the ingestion process has exhausted all data expected from this `Source`

The `Data.Source` interface includes the following:

 * `Data.schema(::Data.Source) => Data.Schema`; typically the `Source` type will store the `Data.Schema` directly, but this isn't strictly required
 * `Data.reset!(::Data.Source)`; used to reset a `Source` type from `READING` or `DONE` to the `BEGINNING` state, ready to be read from again
 * `isdone(::Data.Source, row, col)`; indicates whether the `Source` type is in the `DONE` state; i.e. all data has been exhausted from this source

"""
abstract Source

function reset! end
function isdone end
function reference end

# isdone(stream) = isdone(stream, 1, 1)

abstract StreamType
immutable Field <: StreamType end
immutable Column <: StreamType end

"""
`Data.streamtype{T<:Data.Source, S<:Data.StreamType}(::Type{T}, ::Type{S})` => Bool

Indicates whether the source `T` supports streaming of type `S`. To be overloaded by individual sources according to supported `Data.StreamType`s
"""
function streamtype end

# generic fallback for all Sources
Data.streamtype{T<:StreamType}(source, ::Type{T}) = false

"""
`Data.streamtypes{T<:Data.Sink}(::Type{T})` => Vector{StreamType}

Returns a list of `Data.StreamType`s that the sink supports ingesting; the order of elements indicates the sink's streaming preference
"""
function streamtypes end

function getfield end
function getcolumn end

"""
A `Data.Sink` type represents a data destination; i.e. an "true data source" such as a database, file, API endpoint, etc.

There are two broad types of `Sink`s:

  1) "new sinks": an independent `Sink` constructor creates a *new* "true data source" that can be streamed to
  2) "existing sinks": the `Sink` wraps an already existing "true data source" (or `Source` object that wraps an "true data source").
    Upon construction of these `Sink`s, there is no new creation of "true data source"s; the "ulitmate data source" is simply wrapped to replace or append to

`Sink`s also have notions of state:

  * `BEGINNING`: the `Sink` is freshly constructed and ready to stream data to; this includes initial metadata like column headers
  * `WRITING`: data has been streamed to the `Sink`, but is still open to receive more data
  * `DONE`: the `Sink` has been closed and can no longer receive data

The `Data.Sink` interface includes the following:

 * `Data.schema(::Data.Sink) => Data.Schema`; typically the `Sink` type will store the `Data.Schema` directly, but this isn't strictly required
"""
abstract Sink

"""
`Data.stream!(::Data.Source, ::Data.Sink)` starts transfering data from a newly constructed `Source` type to a newly constructed `Sink` type.
Data transfer typically continues until `isdone(source) == true`, i.e. the `Source` is exhausted, at which point the `Sink` is closed and may
no longer receive data. See individual `Data.stream!` methods for more details on specific `Source`/`Sink` combinations.
"""
function stream!#(::Source, ::Sink)
end

"""
A `Data.Schema` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)
Access to `Data.Schema` fields includes:

 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`
"""
type Schema
    header::Vector{String}       # column names
    types::Vector{DataType}      # Julia types of columns
    rows::Integer                # number of rows in the dataset
    cols::Int                    # number of columns in a dataset
    metadata::Dict{Any, Any}     # for any other metadata we'd like to keep around (not used for '==' operation)
    function Schema(header::Vector, types::Vector{DataType}, rows::Integer=0, metadata::Dict=Dict())
        cols = length(header)
        cols != length(types) && throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
        header = String[string(x) for x in header]
        return new(header, types, rows, cols, metadata)
    end
end

Schema(header, types::Vector{DataType}, rows::Integer=0, meta::Dict=Dict()) = Schema(String[i for i in header], types, rows, meta)
Schema(types::Vector{DataType}, rows::Integer=0, meta::Dict=Dict()) = Schema(String["Column$i" for i = 1:length(types)], types, rows, meta)
const EMPTYSCHEMA = Schema(String[], DataType[], 0, Dict())
Schema() = EMPTYSCHEMA

header(sch::Schema) = sch.header
types(sch::Schema) = sch.types
Base.size(sch::Schema) = (sch.rows, sch.cols)
Base.size(sch::Schema, i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))

function Base.show(io::IO, schema::Schema)
    println(io, "Data.Schema:")
    println(io, "rows: $(schema.rows)\tcols: $(schema.cols)")
    if schema.cols <= 0
        println(io)
    else
        println(io, "Columns:")
        Base.print_matrix(io, hcat(schema.header, schema.types))
    end
end

"Returns the `Data.Schema` for `io`"
schema(io) = io.schema # by default, we assume the `Source`/`Sink` stores the schema directly
"Returns the header/column names (if any) associated with a specific `Source` or `Sink`"
header(io) = header(schema(io))
"Returns the column types associated with a specific `Source` or `Sink`"
types(io) = types(schema(io))
"Returns the (# of rows,# of columns) associated with a specific `Source` or `Sink`"
Base.size(io::Source) = size(schema(io))
Base.size(io::Source, i) = size(schema(io),i)
setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing
setcols!(source, cols) = isdefined(source, :schema) ? (source.schema.cols = cols; nothing) : nothing

# generic definitions
function Data.stream!{T, TT}(source::T, ::Type{TT}, append::Bool, args...)
    typs = Data.streamtypes(TT)
    for typ in typs
        if Data.streamtype(T, typ)
            sink = TT(source, typ, append, args...)
            return Data.stream!(source, typ, sink, append)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $typs"))
end
# for backwards compatibility
Data.stream!{T, TT}(source::T, ::Type{TT}) = Data.stream!(source, TT, false, ())

function Data.stream!{T, TT}(source::T, sink::TT, append::Bool)
    typs = Data.streamtypes(TT)
    for typ in typs
        if Data.streamtype(T, typ)
            sink = TT(sink, source, typ, append)
            return Data.stream!(source, typ, sink, append)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $typs"))
end

# DataFrames DataStreams definitions
using DataFrames, NullableArrays, WeakRefStrings

function Data.schema(df::DataFrame)
    return Data.Schema(map(string, names(df)),
            DataType[eltype(A) <: Nullable ? eltype(eltype(A)) : eltype(A) for A in df.columns], size(df, 1))
end

# DataFrame as a Data.Source
function Data.isdone(source::DataFrame, row, col)
    rows, cols = size(source)
    return row > rows && col > cols
end

Data.streamtype(::Type{DataFrame}, ::Type{Data.Field}) = true
Data.getfield{T}(source::DataFrame, ::Type{T}, row, col) = (@inbounds v = source[row, col]::T; return v)

Data.streamtype(::Type{DataFrame}, ::Type{Data.Column}) = true
Data.getcolumn{T}(source::DataFrame, ::Type{T}, col) = (@inbounds c = source.columns[col]; return c)

# DataFrame as a Data.Sink
function DataFrame{T<:Data.StreamType}(source, ::Type{T}, append, args...)
    sch = Data.schema(source)
    rows, cols = size(sch)
    rows = T === Data.Column ? 0 : rows # don't pre-allocate for Column streaming
    columns = Vector{Any}(cols)
    types = Data.types(sch)
    reference = Data.reference(source)
    for i = 1:cols
        typ = types[i]
        A = Array{typ}(rows)
        columns[i] = NullableArray{typ,1}(A, fill(true, rows), isempty(reference) ? UInt8[] : reference)
    end
    return DataFrame(columns, map(Symbol, Data.header(sch)))
end

Base.empty!(A::NullableArray) = (empty!(A.values); empty!(A.isnull); return A)

# given an existing DataFrame (`sink`), make any necessary changes for streaming `source`
# to it, given we know if we'll be `appending` or not
function DataFrame{T<:Data.StreamType}(sink, source, ::Type{T}, append)
    sch = Data.schema(source)
    rows, cols = size(sch)
    if T === Data.Column
        if !append
            for col in sink.columns
                empty!(col)
            end
        end
    else
        if rows > 0
            newlen = rows + (append ? size(sink, 1) : 0)
            for col in sink.columns
                resize!(col, newlen)
            end
        end
    end
    return sink
end

Data.streamtypes(::Type{DataFrame}) = [Data.Column, Data.Field]

function pushfield!{T}(source, dest::NullableVector{T}, row, col)
    push!(dest, Data.getfield(source, T, row, col))
    return
end

function getfield!{T}(source, dest::NullableVector{T}, row, col, sinkrow)
    @inbounds dest[sinkrow] = Data.getfield(source, T, row, col)
    return
end

function Data.stream!{T}(source::T, ::Type{Data.Field}, sink::DataFrame, append::Bool=true)
    Data.types(source) == Data.types(sink) || throw(ArgumentError("schema mismatch: \n$(Data.schema(source))\nvs.\n$(Data.schema(sink))"))
    rows, cols = size(source)
    Data.isdone(source, 0, 0) && return sink
    columns = sink.columns
    if rows == -1
        sinkrows = size(sink, 1)
        row = 1
        while !Data.isdone(source, row, cols+1)
            for col = 1:cols
                Data.pushfield!(source, columns[col], row, col)
            end
            row += 1
        end
        Data.setrows!(source, sinkrows + row)
    else
        sinkrow = append ? size(sink, 1) - size(source, 1) + 1 : 1
        for row = 1:rows
            for col = 1:cols
                Data.getfield!(source, columns[col], row, col, sinkrow)
            end
            sinkrow += 1
        end
    end
    return sink
end

function pushcolumn!{T}(source, dest::NullableVector{T}, col)
    column = Data.getcolumn(source, T, col)
    append!(dest.values, column.values)
    append!(dest.isnull, column.isnull)
    return length(dest)
end

function pushcolumn!{T}(source, dest::NullableVector{WeakRefString{T}}, col)
    column = Data.getcolumn(source, WeakRefString{T}, col)
    offset = length(dest.values)
    parentoffset = length(dest.parent)
    append!(dest.isnull, column.isnull)
    append!(dest.parent, column.parent)
    # appending new data to `dest` would invalid all existing WeakRefString pointers
    resize!(dest.values, length(dest) + length(column))
    for i = 1:offset
        old = dest.values[i]
        dest.values[i] = WeakRefString{T}(pointer(dest.parent, old.ind), old.len, old.ind)
    end
    for i = 1:length(column)
        old = column.values[i]
        dest.values[offset + i] = WeakRefString{T}(pointer(dest.parent, parentoffset + old.ind), old.len, parentoffset + old.ind)
    end
    return length(dest)
end

function Data.stream!{T}(source::T, ::Type{Data.Column}, sink::DataFrame, append::Bool=true)
    Data.types(source) == Data.types(sink) || throw(ArgumentError("schema mismatch: \n$(Data.schema(source))\nvs.\n$(Data.schema(sink))"))
    rows, cols = size(source)
    Data.isdone(source, 1, 1) && return sink
    columns = sink.columns
    sinkrows = size(sink, 1)
    row = 0
    while !Data.isdone(source, row+1, cols+1)
        for col = 1:cols
            row = Data.pushcolumn!(source, columns[col], col)
        end
    end
    Data.setrows!(source, sinkrows + row)
    return sink
end

end # module Data

end # module DataStreams
