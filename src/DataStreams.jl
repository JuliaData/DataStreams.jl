__precompile__(true)
module DataStreams

export Data, DataFrame

module Data

if !isdefined(Core, :String)
    typealias String UTF8String
end

# Data.Schema
"""
A `Data.Schema` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)
`Data.Schema` allow `Data.Source` and `Data.Sink` to talk to each other and prepare to provide/receive data through streaming.
`Data.Schema` fields include:

 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`; `Nullable{T}` indicates columns that may contain missing data (null values)
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`

`Data.Source` and `Data.Sink` interfaces both require that `Data.schema(source_or_sink)` be defined to ensure
that other `Data.Source`/`Data.Sink` can work appropriately.
"""
type Schema
    header::Vector{String}       # column names
    types::Vector{DataType}      # Julia types of columns
    rows::Int                    # number of rows in the dataset
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

# Data.Source / Data.Sink
abstract Source

function reset! end
function isdone end
reference(x) = UInt8[]

function getfield end
function getcolumn end

abstract Sink

open!(sink, source) = nothing
function streamfield! end
streamfield!{T}(sink, source, ::Type{T}, row, col, cols, sinkrows) = streamfield!(sink, source, T, row, col, cols)
cleanup!(sink) = nothing
flush!(sink) = nothing
close!(sink) = nothing

function streamcolumn! end

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

Returns a list of `Data.StreamType`s that the sink is able to receive; the order of elements indicates the sink's streaming preference
"""
function streamtypes end

schema(source_or_sink) = isdefined(source_or_sink, :schema) ? deepcopy(source_or_sink.schema) : throw(ArgumentError("unable to get schema of $source_or_sink"))
header(source_or_sink) = header(schema(source_or_sink))
types(source_or_sink) = types(schema(source_or_sink))
Base.size(source::Source) = size(schema(source))
Base.size(source::Source, i) = size(schema(source),i)
Base.size(sink::Sink) = size(schema(sink))
Base.size(sink::Sink, i) = size(schema(sink),i)
setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing
setcols!(source, cols) = isdefined(source, :schema) ? (source.schema.cols = cols; nothing) : nothing

# Data.stream!
function stream! end

# generic definitions
function Data.stream!{T, TT}(source::T, ::Type{TT}, append::Bool, args...)
    typs = Data.streamtypes(TT)
    for typ in typs
        if Data.streamtype(T, typ)
            sink = TT(source, typ, append, args...)
            return Data.stream!(source, typ, sink)
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
            return Data.stream!(source, typ, sink)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $typs"))
end
Data.stream!{T, TT <: Data.Sink}(source::T, sink::TT) = Data.stream!(source, sink, false)

# Generic Data.stream! method for Data.Field
function Data.stream!{T1, T2}(source::T1, ::Type{Data.Field}, sink::T2)
    Data.types(source) == Data.types(sink) || throw(ArgumentError("schema mismatch: \n$(Data.schema(source))\nvs.\n$(Data.schema(sink))"))
    Data.isdone(source, 1, 1) && return sink
    rows, cols = size(source)
    sinkrows = max(0, size(sink, 1) - rows)
    types = Data.types(source)
    row = 1
    Data.open!(sink, source)
    try
        if rows == -1
            while true
                for col = 1:cols
                    @inbounds T = types[col]
                    Data.streamfield!(sink, source, T, row, col, cols)
                end
                row += 1
                Data.isdone(source, row, cols) && break
            end
            Data.setrows!(source, row - 1)
        else
            for row = 1:rows, col = 1:cols
                @inbounds T = types[col]
                Data.streamfield!(sink, source, T, row, col, cols, sinkrows)
            end
        end
    catch e
        Data.cleanup!(sink)
        rethrow(e)
    end
    Data.flush!(sink)
    return sink
end

# DataFrames DataStreams definitions
using DataFrames, NullableArrays, CategoricalArrays, WeakRefStrings

# because there's currently not a better place for this to live
import Base.==
=={T}(x::WeakRefString{T}, y::CategoricalArrays.CategoricalValue) = String(x) == String(y)
=={T}(y::CategoricalArrays.CategoricalValue, x::WeakRefString{T}) = String(x) == String(y)

# AbstractColumn definitions
nullcount(A::NullableVector) = sum(A.isnull)
nullcount(A::Vector) = 0
nullcount(A::CategoricalArray) = 0
nullcount(A::NullableCategoricalArray) = sum(A.refs .== 0)
if isdefined(Main, :DataArray)
    nullcount(A::DataArray) = sum(A.na)
end

allocate{T}(::Type{T}, rows, ref) = Array{T}(rows)
function allocate{T}(::Type{Nullable{T}}, rows, ref)
    A = Array{T}(rows)
    return NullableArray{T, 1}(A, fill(true, rows), isempty(ref) ? UInt8[] : ref)
end
allocate{S,R}(::Type{CategoricalValue{S,R}}, rows, ref) = CategoricalArray{S,1,R}(rows)
allocate{S,R}(::Type{Nullable{CategoricalValue{S,R}}}, rows, ref) = NullableCategoricalArray{S,1,R}(rows)

# DataFrames DataStreams implementation
function Data.schema(df::DataFrame)
    return Data.Schema(map(string, names(df)),
            DataType[eltype(A) for A in df.columns], size(df, 1))
end

# DataFrame as a Data.Source
function Data.isdone(source::DataFrame, row, col)
    rows, cols = size(source)
    return row > rows || col > cols
end

Data.streamtype(::Type{DataFrame}, ::Type{Data.Column}) = true
Data.streamtype(::Type{DataFrame}, ::Type{Data.Field}) = true

Data.getcolumn{T}(source::DataFrame, ::Type{T}, col) = (@inbounds A = source.columns[col]; return A)
Data.getcolumn{T}(source::DataFrame, ::Type{Nullable{T}}, col) = (@inbounds A = source.columns[col]::NullableVector{T}; return A)
Data.getcolumn{T,R}(source::DataFrame, ::Type{CategoricalArrays.CategoricalValue{T,R}}, col) = (@inbounds A = source.columns[col]::CategoricalArrays.CategoricalVector{T,R}; return A)
Data.getcolumn{T,R}(source::DataFrame, ::Type{Nullable{CategoricalArrays.CategoricalValue{T,R}}}, col) = (@inbounds A = source.columns[col]::CategoricalArrays.NullableCategoricalVector{T,R}; return A)

Data.getfield{T}(source::DataFrame, ::Type{T}, row, col) = (@inbounds A = Data.getcolumn(source, T, col); return A[row]::T)

# DataFrame as a Data.Sink
DataFrame{T<:Data.StreamType}(so, ::Type{T}, append::Bool, args...) = DataFrame(Data.schema(so), T, Data.reference(so))

function DataFrame{T<:Data.StreamType}(sch::Schema, ::Type{T}=Data.Field, ref::Vector{UInt8}=UInt8[])
    rows, cols = size(sch)
    rows = T === Data.Column || rows < 0 ? 0 : rows # don't pre-allocate for Column streaming
    columns = Vector{Any}(cols)
    types = Data.types(sch)
    for i = 1:cols
        columns[i] = allocate(types[i], rows, ref)
    end
    return DataFrame(columns, map(Symbol, Data.header(sch)))
end

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

function Data.streamfield!{T}(sink::DataFrame, source, ::Type{T}, row, col, cols)
    push!(sink.columns[col], Data.getfield(source, T, row, col))
    return nothing
end

function streamfield{T}(dest, ::Type{T}, row, val)
    @inbounds dest[row] = val
    return nothing
end
function Data.streamfield!{T}(sink::DataFrame, source, ::Type{T}, row, col, cols, sinkrows)
    streamfield(sink.columns[col], T, sinkrows + row, Data.getfield(source, T, row, col))
    return nothing
end

function appendcolumn!{T}(source, ::Type{T}, dest, col)
    column = Data.getcolumn(source, T, col)
    append!(dest, column)
    return length(dest)
end

function appendcolumn!{T}(source, ::Type{Nullable{WeakRefString{T}}}, dest, col)
    column = Data.getcolumn(source, Nullable{WeakRefString{T}}, col)
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

function Data.streamcolumn!{T}(sink::DataFrame, source, ::Type{T}, col, row)
    if row == 0
        sink.columns[col] = Data.getcolumn(source, T, col)
        len = length(sink.columns[col])
    else
        len = appendcolumn!(source, T, sink.columns[col], col)
    end
    return len
end

function Data.stream!{T}(source::T, ::Type{Data.Column}, sink)
    Data.types(source) == Data.types(sink) || throw(ArgumentError("\n\nschema mismatch: \n\nSOURCE: $(Data.schema(source))\n\nvs.\n\nSINK: $(Data.schema(sink))"))
    rows, cols = size(source)
    Data.isdone(source, 1, 1) && return sink
    types = Data.types(source)
    row = cur_row = 0
    for col = 1:cols
        row = Data.streamcolumn!(sink, source, types[col], col, row)
    end
    while !Data.isdone(source, row+1, cols)
        for col = 1:cols
            cur_row = Data.streamcolumn!(sink, source, types[col], col, row)
        end
        row += cur_row
    end
    Data.setrows!(source, row)
    return sink
end

end # module Data

end # module DataStreams
