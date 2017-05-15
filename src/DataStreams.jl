__precompile__(true)
module DataStreams

export Data

module Data

using Compat, Nulls

# Data.Schema
"""
A `Data.Schema` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)

`Data.Schema` allow `Data.Source` and `Data.Sink` to talk to each other and prepare to provide/receive data through streaming.
`Data.Schema` fields include:

 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`; `Nullable{T}` indicates columns that may contain missing data (null values)
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`; note that # of rows may be `null`, meaning unknown
"""
type Schema{R, T}
    header::Vector{String}       # column names
    # types::T                     # Julia types of columns
    rows::(?Int)                 # number of rows in the dataset
    cols::Int                    # number of columns in a dataset
    metadata::Dict               # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String, Int}     # maps column names as Strings to their index # in `header` and `types`
end

function Schema{T <: Tuple}(header::Vector, types::T, rows::?(Integer)=0, metadata::Dict=Dict())
    !isnull(rows) && rows < 0 && throw(ArgumentError("Invalid # of rows for Schema; use `null` to indicate an unknown # of rows"))
    cols = length(header)
    cols != length(types) && throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
    header = String[string(x) for x in header]
    return Schema{!isnull(rows), Tuple{types...}}(header, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header)))
end

Schema(header::Vector, types::Vector, rows::?(Integer)=0, metadata::Dict=Dict()) = Schema(header, Tuple(types), rows, metadata)
Schema(types, rows::?(Integer)=0, meta::Dict=Dict()) = Schema(String["Column$i" for i = 1:length(types)], Tuple(types), rows, meta)
Schema() = Schema(String[], DataType[], null, Dict())

header(sch::Schema) = sch.header
types{R, T}(sch::Schema{R, T}) = Tuple(T.parameters)
Base.size(sch::Schema) = (sch.rows, sch.cols)
Base.size(sch::Schema, i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))

header(source_or_sink) = header(schema(source_or_sink))
setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing

Base.getindex(sch::Schema, col::String) = sch.index[col]

function Base.show{R, T}(io::IO, schema::Schema{R, T})
    println(io, "Data.Schema:")
    println(io, "rows: $(schema.rows)\tcols: $(schema.cols)")
    if schema.cols <= 0
        println(io)
    else
        println(io, "Columns:")
        Base.print_matrix(io, hcat(schema.header, collect(T.parameters)))
    end
end

function transform(sch::Data.Schema, transforms::Dict{Int, Function})
    types = Data.types(sch)
    newtypes = []
    transforms2 = Function[]
    for (i, T) in enumerate(types)
        f = get(transforms, i, identity)
        push!(newtypes, Core.Inference.return_type(f, (T,)))
        push!(transforms2, f)
    end
    return Schema(Data.header(sch), Tuple(newtypes), size(sch, 1), sch.metadata), transforms2
end
transform(sch::Data.Schema, transforms::Dict{String,Function}) = transform(sch, Dict{Int, Function}(sch[x]=>f for (x, f) in transforms))

# Data.StreamTypes
@compat abstract type StreamType end
struct Row <: StreamType end
struct Batch <: StreamType end

# Data.Source Interface
@compat abstract type Source end

# Required methods
function schema end
function isdone end
"""
`Data.streamtype{T<:Data.Source, S<:Data.StreamType}(::Type{T}, ::Type{S})` => Bool

Indicates whether the source `T` supports streaming of type `S`. To be overloaded by individual sources according to supported `Data.StreamType`s
"""
function streamtype end
function streamfrom end

# Generic fallbacks
Data.streamtype{T <: StreamType}(source, ::Type{T}) = false

# Data.Sink Interface
@compat abstract type Sink end

# Required methods
# Sink(sch::Data.Schema, S, append, args...; kwargs...)
# Sink(sink, sch::Data.Schema, S, append)
"""
`Data.streamtypes{T<:Data.Sink}(::Type{T})` => Vector{StreamType}

Returns a list of `Data.StreamType`s that the sink is able to receive; the order of elements indicates the sink's streaming preference
"""
function streamtypes end
function streamto! end

# Optional methods
function cleanup! end
function close! end

# Generic fallbacks
cleanup!(sink) = nothing
close!(sink) = nothing

# Data.stream!
function stream! end

# generic public definitions
const TRUE = x->true
# the 2 methods below are safe and expected to be called from higher-level package convenience functions (e.g. CSV.read)
function Data.stream!{So, Si}(source::So, ::Type{Si}, args...;
                                append::Bool=false,
                                transforms::Dict=Dict{Int, Function}(),
                                filter::Function=TRUE,
                                columns::Vector=[],
                                kwargs...)
    sinkstreamtypes = Data.streamtypes(Si)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(So, sinkstreamtype)
            source_schema = Data.schema(source)
            sink_schema, transforms2 = transform(source_schema, transforms)
            sink = Si(sink_schema, sinkstreamtype, append, args...; kwargs...)
            return Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2, filter, columns)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

function Data.stream!{So, Si}(source::So, sink::Si;
                                append::Bool=false,
                                transforms::Dict=Dict{Int,Function}(),
                                filter::Function=TRUE,
                                columns::Vector=[])
    sinkstreamtypes = Data.streamtypes(Si)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(So, sinkstreamtype)
            source_schema = Data.schema(source)
            sink_schema, transforms2 = transform(source_schema, transforms)
            sink = Si(sink, sink_schema, sinkstreamtype, append)
            return Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2, filter, columns)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

# internal streaming machinery
function generate_inner_loop(knownrows)
    if !knownrows
        return quote
            while true
                @inbounds for col = 1:cols
                    # println("streaming with unknown rows, row = $row, col = $col...")
                    Data.streamto!(sink, Data.Row, source, sourcetypes[col], sinktypes[col], row, col, sink_schema, transforms[col])
                end
                row += 1
                Data.isdone(source, row, cols) && break
            end
            Data.setrows!(source, row - 1)
        end
    else
        return quote
            @inbounds for row = 1:rows, col = 1:cols
                # println("streaming with known rows, sinkrows = $sinkrows, row = $row, col = $col...")
                Data.streamto!(sink, Data.Row, source, sourcetypes[col], sinktypes[col], sinkrows + row, col, sink_schema, transforms[col])
            end
        end
    end
end

@generated function Data.stream!{So, Si, R1, R2, T1, T2}(source::So, ::Type{Data.Row}, sink::Si,
    source_schema::Schema{R1, T1}, sink_schema::Schema{R2, T2},
    transforms, filter, columns)
    return quote
        Data.isdone(source, 1, 1) && return sink
        rows, cols = size(source_schema)
        sinkrows = max(0, size(sink_schema, 1) - rows)
        sourcetypes = $(Tuple(T1.parameters))
        sinktypes = $(Tuple(T2.parameters))
        row = 1
        try
            $(generate_inner_loop(R1))
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
end

function streamto!{T, TT}(sink, ::Type{Data.Row}, source, ::Type{T}, ::Type{TT}, row, col, sch, f)
    val = f(Data.streamfrom(source, Data.Row, T, row, col))
    # println("streamed val = $val")
    return Data.streamto!(sink, Data.Row, val, row, col, sch)
end
streamto!{T <: StreamType}(sink, ::Type{T}, val, row, col, sch) = streamto!(sink, T, val, row, col)

function streamto!{T, TT}(sink, ::Type{Data.Batch}, source, ::Type{T}, ::Type{TT}, row, col, sch, f)
    column = f(Data.streamfrom(source, Data.Batch, T, col)::T)::TT
    return streamto!(sink, Data.Batch, column, row, col, sch)
end

function Data.stream!{T1, T2}(source::T1, ::Type{Data.Batch}, sink::T2, source_schema, sink_schema, transforms)
    Data.isdone(source, 1, 1) && return sink
    rows, cols = size(source_schema)
    sinkrows = max(0, size(sink_schema, 1) - rows)
    sourcetypes = Data.types(source_schema)
    sinktypes = Data.types(sink_schema)
    row = cur_row = 0
    try
        @inbounds for col = 1:cols
            row = Data.streamto!(sink, Data.Batch, source, sourcetypes[col], sinktypes[col], sinkrows + cur_row, col, sink_schema, transforms[col])
        end
        while !Data.isdone(source, row+1, cols)
            @inbounds for col = 1:cols
                cur_row = Data.streamto!(sink, Data.Batch, source, sourcetypes[col], sinktypes[col], sinkrows + row, col, sink_schema, transforms[col])
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

end # module Data

end # module DataStreams
