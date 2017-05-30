__precompile__(true)
module DataStreams

export Data

module Data

using Compat, Nulls, WeakRefStrings

# Data.Schema
"""
A `Data.Schema` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)

`Data.Schema` allow `Data.Source` and `Data.Sink` to talk to each other and prepare to provide/receive data through streaming.
`Data.Schema` fields include:

 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`; `Nullable{T}` indicates columns that may contain missing data (null values)
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`; note that # of rows may be `null`, meaning unknown
"""
type Schema{R <: ?Int, T}
    header::Vector{String}       # column names
    # types::T                     # Julia types of columns
    rows::R                      # number of rows in the dataset
    cols::Int                    # number of columns in a dataset
    metadata::Dict               # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String, Int}     # maps column names as Strings to their index # in `header` and `types`
end

function Schema{T <: Tuple}(header::Vector, types::T, rows::?(Integer)=0, metadata::Dict=Dict())
    !isnull(rows) && rows < 0 && throw(ArgumentError("Invalid # of rows for Schema; use `null` to indicate an unknown # of rows"))
    cols = length(header)
    cols != length(types) && throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
    header = String[string(x) for x in header]
    return Schema{typeof(rows), Tuple{types...}}(header, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header)))
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

function transform{R, T}(sch::Data.Schema{R, T}, transforms::Dict{Int, Function}, weakref)
    types = Data.types(sch)
    transforms2 = ((get(transforms, x, identity) for x = 1:length(types))...)
    newtypes = ((Core.Inference.return_type(transforms2[x], (types[x],)) for x = 1:length(types))...)
    if !weakref
        newtypes = map(x->x >: Null ? ifelse(Nulls.T(x) <: WeakRefString, ?String, x) : ifelse(x <: WeakRefString, String, x), newtypes)
    end
    return Schema(Data.header(sch), newtypes, size(sch, 1), sch.metadata), transforms2
end
transform(sch::Data.Schema, transforms::Dict{String,Function}, s) = transform(sch, Dict{Int, Function}(sch[x]=>f for (x, f) in transforms), s)

# Data.StreamTypes
@compat abstract type StreamType end
struct Field <: StreamType end
struct Column <: StreamType end

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

struct RandomAccess end
struct Sequential end
accesspattern(x) = Sequential()

const EMPTY_REFERENCE = UInt8[]
reference(x) = EMPTY_REFERENCE

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
close!(sink) = sink

# Data.stream!
function stream! end

weakrefstrings(x) = false

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
            wk = weakrefstrings(Si)
            sink_schema, transforms2 = Data.transform(source_schema, transforms, wk)
            if wk
                sink = Si(sink_schema, sinkstreamtype, append, args...; reference=Data.reference(source), kwargs...)
            else
                sink = Si(sink_schema, sinkstreamtype, append, args...; kwargs...)
            end
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
            wk = weakrefstrings(Si)
            sink_schema, transforms2 = transform(source_schema, transforms, wk)
            if wk
                sink = Si(sink, sink_schema, sinkstreamtype, append; reference=Data.reference(source))
            else
                sink = Si(sink, sink_schema, sinkstreamtype, append)
            end
            return Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2, filter, columns)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

# variables
  # row vs. batch
  # known source rows
  # column selecting
  # RandomAccess
  # row filtering
  # transform functions
#

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

# WeakRefStringArray
 # pass tuple of reference columns to Sink constructor
 # sink constructor needs to allocate WeakRefStringArray w/ source reference columns
 # setindex(A, WeakRefString(ptr, idx, len), i)
 # CSV.parsefield(src, WeakRefString) => WeakRefString(ptr, idx, len)
 # keep Data.reference?

@generated function Data.stream!{So, Si, T1}(source::So, ::Type{Data.Field}, sink::Si,
    source_schema::Schema{Int, T1}, sink_schema, transforms, filter, columns)
    sourcetypes = Tuple(T1.parameters)
    N = length(sourcetypes)
    return quote
        Data.isdone(source, 1, 1) && return sink
        rows, cols = size(source_schema)::Tuple{Int, Int}
        sinkrows = max(0, size(sink_schema, 1)::Int - rows)
        sourcetypes = $sourcetypes
        try
            @inbounds for row = 1:rows
                Base.@nexprs $N col->begin
                        val_col = Data.streamfrom(source, Data.Field, sourcetypes[col], sinkrows + row, col)
                        # hack to improve codegen due to inability of inference to inline Union{T, Null} val_col here
                        if val_col isa Null
                            Data.streamto!(sink, Data.Field, transforms[col](val_col), sinkrows + row, col, Val{true})
                        else
                            Data.streamto!(sink, Data.Field, transforms[col](val_col), sinkrows + row, col, Val{true})
                        end
                    # val_col = transforms[col](Data.streamfrom(source, Data.Field, sourcetypes[col], sinkrows + row, col))
                    # Data.streamto!(sink, Data.Field, val_col, sinkrows + row, col, Val{true})
                end
            end
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
end

@generated function Data.stream!{So, Si, T1}(source::So, ::Type{Data.Field}, sink::Si,
    source_schema::Schema{Null, T1}, sink_schema, transforms, filter, columns)
    sourcetypes = Tuple(T1.parameters)
    N = length(sourcetypes)
    return quote
        Data.isdone(source, 1, 1) && return sink
        rows, cols = size(source_schema)
        sinkrows = max(0, size(sink_schema, 1))
        sourcetypes = $sourcetypes
        try
            $(
            # unknown # of rows
            quote
                row = 1
                while true
                    Base.@nexprs $N col->begin
                        val_col = transforms[col](Data.streamfrom(source, Data.Field, sourcetypes[col], sinkrows + row, col))
                        Data.streamto!(sink, Data.Field, val_col, sinkrows + row, Val{col}, Val{false})
                    end
                    Data.isdone(source, row, cols) && break
                    row += 1
                end
            end
            )
            Data.setrows!(source, row)
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
end

@generated function Data.stream!{So, Si, R1, T1}(source::So, ::Type{Data.Column}, sink::Si,
    source_schema::Schema{R1, T1}, sink_schema, transforms, filter, columns)
    sourcetypes = Tuple(T1.parameters)
    N = length(sourcetypes)
    return quote
        Data.isdone(source, 1, 1) && return sink
        rows, cols = size(source_schema)
        sinkrows = max(0, size(sink_schema, 1) - rows)
        sourcetypes = $sourcetypes
        try
            $(quote
                row = cur_row = 0
                while !Data.isdone(source, row+1, cols)
                    Base.@nexprs $N col->begin
                        column_col = transforms[col](Data.streamfrom(source, Data.Column, sourcetypes[col], col))
                        cur_row = Data.streamto!(sink, Data.Column, column_col, sinkrows + row, col, $(Val{R1}))
                    end
                    row += cur_row
                end
            end)
            Data.setrows!(source, row)
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
end

# function streamto!{T, TT}(sink, ::Type{Data.Field}, source, ::Type{T}, ::Type{TT}, row, col, sch, f)
#     val = f(Data.streamfrom(source, Data.Field, T, row, col))
#     # println("streamed val = $val")
#     return Data.streamto!(sink, Data.Field, val, row, col, sch)
# end
# streamto!{T <: StreamType}(sink, ::Type{T}, val, row, col, knownrows) = streamto!(sink, T, val, row, col)

# function streamto!{T, TT}(sink, ::Type{Data.Column}, source, ::Type{T}, ::Type{TT}, row, col, sch, f)
#     column = f(Data.streamfrom(source, Data.Column, T, col)::T)::TT
#     return streamto!(sink, Data.Column, column, row, col, sch)
# end
#
# function Data.stream!{T1, T2}(source::T1, ::Type{Data.Column}, sink::T2, source_schema, sink_schema, transforms)
#     Data.isdone(source, 1, 1) && return sink
#     rows, cols = size(source_schema)
#     sinkrows = max(0, size(sink_schema, 1) - rows)
#     sourcetypes = Data.types(source_schema)
#     sinktypes = Data.types(sink_schema)
#     row = cur_row = 0
#     try
#         @inbounds for col = 1:cols
#             row = Data.streamto!(sink, Data.Column, source, sourcetypes[col], sinktypes[col], sinkrows + cur_row, col, sink_schema, transforms[col])
#         end
#         while !Data.isdone(source, row+1, cols)
#             @inbounds for col = 1:cols
#                 cur_row = Data.streamto!(sink, Data.Column, source, sourcetypes[col], sinktypes[col], sinkrows + row, col, sink_schema, transforms[col])
#             end
#             row += cur_row
#         end
#         Data.setrows!(source, row)
#     catch e
#         Data.cleanup!(sink)
#         rethrow(e)
#     end
#     return sink
# end

end # module Data

end # module DataStreams
