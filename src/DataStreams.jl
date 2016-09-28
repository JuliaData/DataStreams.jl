__precompile__(true)
module DataStreams

export Data, DataFrame

module Data

if !isdefined(Core, :String)
    typealias String UTF8String
end

abstract StreamType
immutable Field <: StreamType end
immutable Column <: StreamType end

# Data.Schema
"""
A `Data.Schema{T}` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)

`Data.Schema` allow `Data.Source` and `Data.Sink` to talk to each other and prepare to provide/receive data through streaming.
`Data.Schema` fields include:

 * a type parameter `{T}` that indicates the `Data.StreamType` of the `Data.Schema`
 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`; `Nullable{T}` indicates columns that may contain missing data (null values)
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`

`Data.Source` and `Data.Sink` interfaces both require that `Data.schema(source_or_sink)` be defined to ensure
that other `Data.Source`/`Data.Sink` can work appropriately.
"""
type Schema{T <: StreamType}
    header::Vector{String}       # column names
    types::Vector{DataType}      # Julia types of columns
    rows::Int                    # number of rows in the dataset
    cols::Int                    # number of columns in a dataset
    metadata::Dict{Any, Any}     # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String,Int}      # maps column names as Strings to their index # in `header` and `types`
end

function Schema{T <: StreamType}(::Type{T}, header::Vector, types::Vector{DataType}, rows::Integer=0, metadata::Dict=Dict())
    cols = length(header)
    cols != length(types) && throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
    header = String[string(x) for x in header]
    return Schema{T}(header, types, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header)))
end

Schema{T <: StreamType}(::Type{T}, types::Vector{DataType}, rows::Integer=0, meta::Dict=Dict()) = Schema(T, String["Column$i" for i = 1:length(types)], types, rows, meta)
Schema() = Schema(Field, String[], DataType[], 0, Dict())

header(sch::Schema) = sch.header
types(sch::Schema) = sch.types
Base.size(sch::Schema) = (sch.rows, sch.cols)
Base.size(sch::Schema, i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))

Base.getindex(sch::Schema, col::String) = sch.index[col]

function Base.show{T}(io::IO, schema::Schema{T})
    println(io, "Data.Schema{$T}:")
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

cleanup!(sink) = nothing
close!(sink) = nothing

function streamfield! end
function streamcolumn! end

function transform{S, F}(sch::Data.Schema{S}, transforms::Dict{Int,F})
    types = Data.types(sch)
    newtypes = similar(types)
    transforms2 = Array{Function}(length(types))
    for (i, T) in enumerate(types)
        f = get(transforms, i, identity)
        newtypes[i] = Core.Inference.return_type(f, (T,))
        transforms2[i] = f
    end
    return Schema(S, Data.header(sch), newtypes, size(sch, 1), sch.metadata), transforms2
end
transform{F}(sch::Data.Schema, transforms::Dict{String,F}) = transform(sch, Dict(sch[x]=>f for (x,f) in transforms))

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

streamtype{T}(sch::Schema{T}) = T

transform{T <: AbstractVector}(::Type{T}, ::Type{Data.Column}) = T
transform{T <: AbstractVector}(::Type{T}, ::Type{Data.Field}) = eltype(T)
transform{T, S}(::Type{T}, ::Type{S}) = T

function schema{S <: StreamType}(source, ::Type{S})
    sch = Data.schema(source)
    T = streamtype(sch)
    T == S && return sch
    types = DataType[transform(TT, S) for TT in sch.types]
    return Schema(S, Data.header(sch), types, size(sch, 1), sch.metadata)
end

schema(source) = isdefined(source, :schema) ? source.schema : throw(ArgumentError("can't get Data.Schema of $source"))
header(source_or_sink) = header(schema(source_or_sink))
types{T <: StreamType}(source_or_sink, ::Type{T}=Field) = types(schema(source_or_sink, T))
Base.size(source_or_sink::Union{Source,Sink}) = size(schema(source_or_sink))
Base.size(source_or_sink::Union{Source,Sink}, i) = size(schema(source_or_sink),i)
setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing
setcols!(source, cols) = isdefined(source, :schema) ? (source.schema.cols = cols; nothing) : nothing

# Data.stream!
function stream! end

# generic definitions
function Data.stream!{So, Si}(source::So, ::Type{Si}, append::Bool, transforms::Dict, args...; kwargs...)
    sinkstreamtypes = Data.streamtypes(Si)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(So, sinkstreamtype)
            transformed_schema, transforms2 = transform(Data.schema(source, sinkstreamtype), transforms)
            sink = Si(transformed_schema, sinkstreamtype, append, reference(source), args...; kwargs...)
            return Data.stream!(source, sinkstreamtype, sink, transforms2)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end
# for backwards compatibility
Data.stream!{So, Si}(source::So, ::Type{Si}) = Data.stream!(source, Si, false, Dict{Int,Function}())

function Data.stream!{So, Si}(source::So, sink::Si, append::Bool=false, transforms::Dict=Dict{Int,Function}())
    sinkstreamtypes = Data.streamtypes(Si)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(So, sinkstreamtype)
            transformed_schema, transforms2 = transform(Data.schema(source, sinkstreamtype), transforms)
            sink = Si(sink, transformed_schema, sinkstreamtype, append, reference(source))
            return Data.stream!(source, sinkstreamtype, sink, transforms2)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

function stream!{T, TT}(::Type{Data.Field}, sink, source, ::Type{T}, ::Type{TT}, row, col, cols, f, push)
    val = f(Data.getfield(source, T, row, col)::T)::TT
    return stream!(sink, Data.Field, val, row, col, cols, push)
end
function stream!{T, TT}(::Type{Data.Field}, sink, source, ::Type{T}, ::Type{TT}, row, col, cols, f)
    val = f(Data.getfield(source, T, row, col)::T)::TT
    return stream!(sink, Data.Field, val, row, col, cols)
end
stream!{T <: StreamType}(sink, ::Type{T}, val, row, col, cols, push) = stream!(sink, T, val, row, col, cols)

# Generic Data.stream! method for Data.Field
function Data.stream!{T1, T2}(source::T1, ::Type{Data.Field}, sink::T2, transforms)
    Data.isdone(source, 1, 1) && return sink
    rows, cols = size(source)
    sinkrows = max(0, size(sink, 1) - rows)
    sourcetypes = Data.types(source)
    sinktypes = Data.types(sink)
    row = 1
    try
        if rows == -1
            while true
                @inbounds for col = 1:cols
                    Data.stream!(Data.Field, sink, source, sourcetypes[col], sinktypes[col], row, col, cols, transforms[col], true)
                end
                row += 1
                Data.isdone(source, row, cols) && break
            end
            Data.setrows!(source, row - 1)
        else
            @inbounds for row = 1:rows, col = 1:cols
                Data.stream!(Data.Field, sink, source, sourcetypes[col], sinktypes[col], sinkrows + row, col, cols, transforms[col])
            end
        end
    catch e
        Data.cleanup!(sink)
        rethrow(e)
    end
    return sink
end

function stream!{T, TT}(::Type{Data.Column}, sink, source, ::Type{T}, ::Type{TT}, row, col, cols, f, push)
    column = f(Data.getcolumn(source, T, col)::T)::TT
    return stream!(sink, Data.Column, column, row, col, cols, push)
end
function stream!{T, TT}(::Type{Data.Column}, sink, source, ::Type{T}, ::Type{TT}, row, col, cols, f)
    column = f(Data.getcolumn(source, T, col)::T)::TT
    return stream!(sink, Data.Column, column, row, col, cols)
end

function Data.stream!{T1, T2}(source::T1, ::Type{Data.Column}, sink::T2, transforms)
    rows, cols = size(source)
    Data.isdone(source, 1, 1) && return sink
    sourcetypes = Data.types(source, Data.Column)
    sinktypes = Data.types(sink, Data.Column)
    row = cur_row = 0
    @inbounds for col = 1:cols
        row = Data.stream!(Data.Column, sink, source, sourcetypes[col], sinktypes[col], cur_row, col, cols, transforms[col])
    end
    while !Data.isdone(source, row+1, cols)
        @inbounds for col = 1:cols
            cur_row = Data.stream!(Data.Column, sink, source, sourcetypes[col], sinktypes[col], row, col, cols, transforms[col], true)
        end
        row += cur_row
    end
    Data.setrows!(source, row)
    return sink
end

# DataFrames DataStreams definitions
using DataFrames, NullableArrays, CategoricalArrays, WeakRefStrings

# DataFrames DataStreams implementation
function Data.schema(df::DataFrame)
    return Data.Schema(Data.Column, map(string, names(df)),
            DataType[typeof(A) for A in df.columns], size(df, 1))
end

# DataFrame as a Data.Source
function Data.isdone(source::DataFrame, row, col)
    rows, cols = size(source)
    return row > rows || col > cols
end

Data.streamtype(::Type{DataFrame}, ::Type{Data.Column}) = true
Data.streamtype(::Type{DataFrame}, ::Type{Data.Field}) = true

Data.getcolumn{T <: AbstractVector}(source::DataFrame, ::Type{T}, col) = (@inbounds A = source.columns[col]::T; return A)
Data.getcolumn{T}(source::DataFrame, ::Type{T}, col) = (@inbounds A = source.columns[col]; return A)
Data.getfield{T}(source::DataFrame, ::Type{T}, row, col) = (@inbounds A = Data.getcolumn(source, T, col); return A[row]::T)

# DataFrame as a Data.Sink
allocate{T}(::Type{T}, rows, ref) = Array{T}(rows)
allocate{T}(::Type{Vector{T}}, rows, ref) = Array{T}(rows)

allocate{T}(::Type{Nullable{T}}, rows, ref) = NullableArray{T, 1}(Array{T}(rows), fill(true, rows), isempty(ref) ? UInt8[] : ref)
allocate{T}(::Type{NullableVector{T}}, rows, ref) = NullableArray{T, 1}(Array{T}(rows), fill(true, rows), isempty(ref) ? UInt8[] : ref)

allocate{S,R}(::Type{CategoricalArrays.CategoricalValue{S,R}}, rows, ref) = CategoricalArray{S,1,R}(rows)
allocate{S,R}(::Type{CategoricalVector{S,R}}, rows, ref) = CategoricalArray{S,1,R}(rows)

allocate{S,R}(::Type{Nullable{CategoricalArrays.CategoricalValue{S,R}}}, rows, ref) = NullableCategoricalArray{S,1,R}(rows)
allocate{S,R}(::Type{NullableCategoricalVector{S,R}}, rows, ref) = NullableCategoricalArray{S,1,R}(rows)

if isdefined(Main, :DataArray)
    allocate{T}(::Type{DataVector{T}}, rows, ref) = DataArray{T}(rows)
end

function DataFrame{T <: Data.StreamType}(sch::Data.Schema, ::Type{T}=Data.Field, append::Bool=false, ref::Vector{UInt8}=UInt8[])
    rows, cols = size(sch)
    rows = max(0, T === Data.Column ? 0 : rows) # don't pre-allocate for Column streaming
    columns = Vector{Any}(cols)
    types = Data.types(sch)
    for i = 1:cols
        columns[i] = allocate(types[i], rows, ref)
    end
    return DataFrame(columns, map(Symbol, Data.header(sch)))
end

# given an existing DataFrame (`sink`), make any necessary changes for streaming source
# with Data.Schema `sch` to it, given we know if we'll be `appending` or not
function DataFrame(sink, sch::Data.Schema, ::Type{Field}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    foreach(x->resize!(x, max(0, rows + (append ? size(sink, 1) : 0))), sink.columns)
    return sink
end
function DataFrame(sink, sch::Schema, ::Type{Column}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    !append && foreach(empty!, sink.columns)
    return sink
end

Data.streamtypes(::Type{DataFrame}) = [Data.Column, Data.Field]

Data.stream!{T}(sink::DataFrame, ::Type{Data.Field}, val::T, row, col, cols, push::Bool) = push!(sink.columns[col]::Vector{T}, val)
Data.stream!{T}(sink::DataFrame, ::Type{Data.Field}, val::Nullable{T}, row, col, cols, push::Bool) = push!(sink.columns[col]::NullableVector{T}, val)
Data.stream!{T, R}(sink::DataFrame, ::Type{Data.Field}, val::CategoricalValue{T, R}, row, col, cols, push::Bool) = push!(sink.columns[col]::CategoricalVector{T, R}, val)
Data.stream!{T, R}(sink::DataFrame, ::Type{Data.Field}, val::Nullable{CategoricalValue{T, R}}, row, col, cols, push::Bool) = push!(sink.columns[col]::NullableCategoricalVector{T, R}, val)

Data.stream!{T}(sink::DataFrame, ::Type{Data.Field}, val::T, row, col, cols) = (sink.columns[col]::Vector{T})[row] = val
Data.stream!{T}(sink::DataFrame, ::Type{Data.Field}, val::Nullable{T}, row, col, cols) = (sink.columns[col]::NullableVector{T})[row] = val
Data.stream!{T, R}(sink::DataFrame, ::Type{Data.Field}, val::CategoricalValue{T, R}, row, col, cols) = (sink.columns[col]::CategoricalVector{T, R})[row] = val
Data.stream!{T, R}(sink::DataFrame, ::Type{Data.Field}, val::Nullable{CategoricalValue{T, R}}, row, col, cols) = (sink.columns[col]::NullableCategoricalVector{T, R})[row] = val

function Data.stream!{T}(sink::DataFrame, ::Type{Data.Column}, column::T, row, col, cols)
    sink.columns[col] = column
    return length(column)
end

function Data.stream!{T}(sink::DataFrame, ::Type{Data.Column}, column::T, row, col, cols, push::Bool)
    append!(sink.columns[col]::T, column)
    return length(column)
end

function Base.append!{T}(dest::NullableVector{WeakRefString{T}}, column::NullableVector{WeakRefString{T}})
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

end # module Data

end # module DataStreams
