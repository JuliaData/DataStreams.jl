__precompile__(true)
module DataStreams

export Data, DataTable

module Data

abstract StreamType
immutable Field <: StreamType end
immutable Column <: StreamType end

# Data.Schema
"""
A `Data.Schema` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)

`Data.Schema` allow `Data.Source` and `Data.Sink` to talk to each other and prepare to provide/receive data through streaming.
`Data.Schema` fields include:

 * A boolean type parameter that indicates whether the # of rows is known in the `Data.Source`; this is useful as a type parameter to allow `Data.Sink` and `Data.streamto!` methods to dispatch. Note that the sentinel value `-1` is used as the # of rows when the # of rows is unknown.
 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`; `Nullable{T}` indicates columns that may contain missing data (null values)
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`

`Data.Source` and `Data.Sink` interfaces both require that `Data.schema(source_or_sink)` be defined to ensure
that other `Data.Source`/`Data.Sink` can work appropriately.
"""
type Schema{RowsAreKnown}
    header::Vector{String}       # column names
    types::Vector{DataType}      # Julia types of columns
    rows::Int                    # number of rows in the dataset
    cols::Int                    # number of columns in a dataset
    metadata::Dict{Any, Any}     # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String,Int}      # maps column names as Strings to their index # in `header` and `types`
end

function Schema(header::Vector, types::Vector{DataType}, rows::Integer=0, metadata::Dict=Dict())
    rows < -1 && throw(ArgumentError("Invalid # of rows for Schema; use -1 to indicate an unknown # of rows"))
    cols = length(header)
    cols != length(types) && throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
    header = String[string(x) for x in header]
    return Schema{rows > -1}(header, types, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header)))
end

Schema(types::Vector{DataType}, rows::Integer=0, meta::Dict=Dict()) = Schema(String["Column$i" for i = 1:length(types)], types, rows, meta)
Schema() = Schema(String[], DataType[], 0, Dict())

header(sch::Schema) = sch.header
types(sch::Schema) = sch.types
Base.size(sch::Schema) = (sch.rows, sch.cols)
Base.size(sch::Schema, i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))

header(source_or_sink) = header(schema(source_or_sink))
types{T <: StreamType}(source_or_sink, ::Type{T}=Field) = types(schema(source_or_sink, T))
setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing

Base.getindex(sch::Schema, col::String) = sch.index[col]

function Base.show{b}(io::IO, schema::Schema{b})
    println(io, "Data.Schema{$(b)}:")
    println(io, "rows: $(schema.rows)\tcols: $(schema.cols)")
    if schema.cols <= 0
        println(io)
    else
        println(io, "Columns:")
        Base.print_matrix(io, hcat(schema.header, schema.types))
    end
end

function transform(sch::Data.Schema, transforms::Dict{Int,Function})
    types = Data.types(sch)
    newtypes = similar(types)
    transforms2 = Array{Function}(length(types))
    for (i, T) in enumerate(types)
        f = get(transforms, i, identity)
        newtypes[i] = Core.Inference.return_type(f, (T,))
        transforms2[i] = f
    end
    return Schema(Data.header(sch), newtypes, size(sch, 1), sch.metadata), transforms2
end
transform(sch::Data.Schema, transforms::Dict{String,Function}) = transform(sch, Dict{Int,Function}(sch[x]=>f for (x,f) in transforms))

# Data.Source Interface
abstract Source

# Required methods
function schema end
function isdone end
"""
`Data.streamtype{T<:Data.Source, S<:Data.StreamType}(::Type{T}, ::Type{S})` => Bool

Indicates whether the source `T` supports streaming of type `S`. To be overloaded by individual sources according to supported `Data.StreamType`s
"""
function streamtype end
function streamfrom end

# Optional method
# size(source)
# size(source, i)
function reference end

# Generic fallbacks
Base.size(s::Source) = size(schema(s, Data.Field))
Base.size(s::Source, i) = size(schema(s, Data.Field), i)
schema(source) = schema(source, Data.Field)
function schema(source, ::Type{Data.Field})
    sch = Data.schema(source, Data.Column)
    types = DataType[eltype(TT) for TT in Data.types(sch)]
    return Schema(Data.header(sch), types, size(sch, 1), sch.metadata)
end
Data.streamtype{T <: StreamType}(source, ::Type{T}) = false
reference(x) = UInt8[]
#TODO: fallback for streamfrom for Nullables

# Data.Sink Interface
abstract Sink

# Required methods
# Sink(sch::Data.Schema, S, append, ref, args...; kwargs...)
# Sink(sink, sch::Data.Schema, S, append, ref)
"""
`Data.streamtypes{T<:Data.Sink}(::Type{T})` => Vector{StreamType}

Returns a list of `Data.StreamType`s that the sink is able to receive; the order of elements indicates the sink's streaming preference
"""
function streamtypes end
function streamto! end

# Optional methods
# size(source)
# size(source, i)
function cleanup! end
function close! end

# Generic fallbacks
cleanup!(sink) = nothing
close!(sink) = nothing

# Data.stream!
function stream! end

# generic definitions
function Data.stream!{So, Si}(source::So, ::Type{Si}, append::Bool, transforms::Dict, args...; kwargs...)
    sinkstreamtypes = Data.streamtypes(Si)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(So, sinkstreamtype)
            source_schema = Data.schema(source, sinkstreamtype)
            sink_schema, transforms2 = transform(source_schema, transforms)
            sink = Si(sink_schema, sinkstreamtype, append, reference(source), args...; kwargs...)
            return Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2)
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
            source_schema = Data.schema(source, sinkstreamtype)
            sink_schema, transforms2 = transform(source_schema, transforms)
            sink = Si(sink, sink_schema, sinkstreamtype, append, reference(source))
            return Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

function streamto!{T, TT}(sink, ::Type{Data.Field}, source, ::Type{T}, ::Type{TT}, row, col, sch, f)
    val = f(Data.streamfrom(source, Data.Field, T, row, col))
    return Data.streamto!(sink, Data.Field, val, row, col, sch)
end
streamto!{T <: StreamType}(sink, ::Type{T}, val, row, col, sch) = streamto!(sink, T, val, row, col)

# Generic Data.stream! method for Data.Field
function Data.stream!{T1, T2}(source::T1, ::Type{Data.Field}, sink::T2, source_schema::Schema{true}, sink_schema, transforms)
    Data.isdone(source, 1, 1) && return sink
    rows, cols = size(source_schema)
    sinkrows = max(0, size(sink_schema, 1) - rows)
    sourcetypes = Data.types(source_schema)
    sinktypes = Data.types(sink_schema)
    try
        @inbounds for row = 1:rows, col = 1:cols
            Data.streamto!(sink, Data.Field, source, sourcetypes[col], sinktypes[col], sinkrows + row, col, sink_schema, transforms[col])
        end
    catch e
        Data.cleanup!(sink)
        rethrow(e)
    end
    return sink
end
function Data.stream!{T1, T2}(source::T1, ::Type{Data.Field}, sink::T2, source_schema::Schema{false}, sink_schema, transforms)
    Data.isdone(source, 1, 1) && return sink
    rows, cols = size(source_schema)
    sourcetypes = Data.types(source_schema)
    sinktypes = Data.types(sink_schema)
    row = 1
    try
        while true
            @inbounds for col = 1:cols
                Data.streamto!(sink, Data.Field, source, sourcetypes[col], sinktypes[col], row, col, sink_schema, transforms[col])
            end
            row += 1
            Data.isdone(source, row, cols) && break
        end
        Data.setrows!(source, row - 1)
    catch e
        Data.cleanup!(sink)
        rethrow(e)
    end
    return sink
end

function streamto!{T, TT}(sink, ::Type{Data.Column}, source, ::Type{T}, ::Type{TT}, row, col, sch, f)
    column = f(Data.streamfrom(source, Data.Column, T, col)::T)::TT
    return streamto!(sink, Data.Column, column, row, col, sch)
end

function Data.stream!{T1, T2}(source::T1, ::Type{Data.Column}, sink::T2, source_schema, sink_schema, transforms)
    Data.isdone(source, 1, 1) && return sink
    rows, cols = size(source_schema)
    sinkrows = max(0, size(sink_schema, 1) - rows)
    sourcetypes = Data.types(source_schema)
    sinktypes = Data.types(sink_schema)
    row = cur_row = 0
    try
        @inbounds for col = 1:cols
            row = Data.streamto!(sink, Data.Column, source, sourcetypes[col], sinktypes[col], sinkrows + cur_row, col, sink_schema, transforms[col])
        end
        while !Data.isdone(source, row+1, cols)
            @inbounds for col = 1:cols
                cur_row = Data.streamto!(sink, Data.Column, source, sourcetypes[col], sinktypes[col], sinkrows + row, col, sink_schema, transforms[col])
            end
            row += cur_row
        end
        Data.setrows!(source, row)
    catch e
        Data.cleanup!(sink)
        rethrow(e)
    end
    return sink
end

# DataTables DataStreams definitions
using DataTables, NullableArrays, CategoricalArrays, WeakRefStrings

# DataTables DataStreams implementation
function Data.schema(df::DataTable, ::Type{Data.Column})
    return Data.Schema(map(string, names(df)),
            DataType[typeof(A) for A in df.columns], size(df, 1))
end

# DataTable as a Data.Source
function Data.isdone(source::DataTable, row, col)
    rows, cols = size(source)
    return row > rows || col > cols
end

Data.streamtype(::Type{DataTable}, ::Type{Data.Column}) = true
Data.streamtype(::Type{DataTable}, ::Type{Data.Field}) = true

Data.streamfrom{T <: AbstractVector}(source::DataTable, ::Type{Data.Column}, ::Type{T}, col) = (@inbounds A = source.columns[col]::T; return A)
Data.streamfrom{T}(source::DataTable, ::Type{Data.Column}, ::Type{T}, col) = (@inbounds A = source.columns[col]; return A)
Data.streamfrom{T}(source::DataTable, ::Type{Data.Field}, ::Type{T}, row, col) = (@inbounds A = Data.streamfrom(source, Data.Column, T, col); return A[row]::T)

# DataTable as a Data.Sink
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

function DataTable{T <: Data.StreamType}(sch::Data.Schema, ::Type{T}=Data.Field, append::Bool=false, ref::Vector{UInt8}=UInt8[], args...)
    rows, cols = size(sch)
    rows = max(0, T <: Data.Column ? 0 : rows) # don't pre-allocate for Column streaming
    columns = Vector{Any}(cols)
    types = Data.types(sch)
    for i = 1:cols
        columns[i] = allocate(types[i], rows, ref)
    end
    return DataTable(columns, map(Symbol, Data.header(sch)))
end

# given an existing DataTable (`sink`), make any necessary changes for streaming source
# with Data.Schema `sch` to it, given we know if we'll be `appending` or not
function DataTable(sink, sch::Data.Schema, ::Type{Field}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    newsize = max(0, rows) + (append ? size(sink, 1) : 0)
    # need to make sure we don't break a NullableVector{WeakRefString{UInt8}} when appending
    if append
        for (i, T) in enumerate(Data.types(sch))
            if T <: Nullable{WeakRefString{UInt8}}
                sink.columns[i] = NullableArray(String[string(get(x, "")) for x in sink.columns[i]])
                sch.types[i] = Nullable{String}
            end
        end
    end
    newsize != size(sink, 1) && foreach(x->resize!(x, newsize), sink.columns)
    sch.rows = newsize
    return sink
end
function DataTable(sink, sch::Schema, ::Type{Column}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    append ? (sch.rows += size(sink, 1)) : foreach(empty!, sink.columns)
    return sink
end

Data.streamtypes(::Type{DataTable}) = [Data.Column, Data.Field]

Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::T, row, col, sch::Data.Schema{false}) = push!(sink.columns[col]::Vector{T}, val)
Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::Nullable{T}, row, col, sch::Data.Schema{false}) = push!(sink.columns[col]::NullableVector{T}, val)
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::CategoricalValue{T, R}, row, col, sch::Data.Schema{false}) = push!(sink.columns[col]::CategoricalVector{T, R}, val)
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::Nullable{CategoricalValue{T, R}}, row, col, sch::Data.Schema{false}) = push!(sink.columns[col]::NullableCategoricalVector{T, R}, val)

Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::T, row, col, sch::Data.Schema{true}) = (sink.columns[col]::Vector{T})[row] = val
Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::Nullable{T}, row, col, sch::Data.Schema{true}) = (sink.columns[col]::NullableVector{T})[row] = val
Data.streamto!(sink::DataTable, ::Type{Data.Field}, val::Nullable{WeakRefString{UInt8}}, row, col, sch::Data.Schema{true}) = (sink.columns[col][row] = val)
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::CategoricalValue{T, R}, row, col, sch::Data.Schema{true}) = (sink.columns[col]::CategoricalVector{T, R})[row] = val
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::Nullable{CategoricalValue{T, R}}, row, col, sch::Data.Schema{true}) = (sink.columns[col]::NullableCategoricalVector{T, R})[row] = val

function Data.streamto!{T}(sink::DataTable, ::Type{Data.Column}, column::T, row, col, sch::Data.Schema)
    if row == 0
        sink.columns[col] = column
    else
        append!(sink.columns[col]::T, column)
    end
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
