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
mutable struct Schema{R <: ?Int, T}
    header::Vector{String}       # column names
    # types::T                     # Julia types of columns
    rows::R                      # number of rows in the dataset
    cols::Int                    # number of columns in a dataset
    metadata::Dict               # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String, Int}     # maps column names as Strings to their index # in `header` and `types`
end

function Schema{T <: Tuple}(header::Vector, types::T, rows::(?Integer)=0, metadata::Dict=Dict())
    !isnull(rows) && rows < 0 && throw(ArgumentError("Invalid # of rows for Schema; use `null` to indicate an unknown # of rows"))
    cols = length(header)
    cols != length(types) && throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
    header = String[string(x) for x in header]
    return Schema{typeof(rows), Tuple{types...}}(header, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header)))
end

Schema(header::Vector, types::Vector, rows::(?Integer)=0, metadata::Dict=Dict()) = Schema(header, Tuple(types), rows, metadata)
Schema(types, rows::(?Integer)=0, meta::Dict=Dict()) = Schema(String["Column$i" for i = 1:length(types)], Tuple(types), rows, meta)
Schema() = Schema(String[], DataType[], null, Dict())

header(sch::Schema) = sch.header
types{R, T}(sch::Schema{R, T}) = Tuple(T.parameters)
Base.size(sch::Schema) = (sch.rows, sch.cols)
Base.size(sch::Schema, i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))

setrows!(source, rows) = isdefined(source, :schema) ? (source.schema.rows = rows; nothing) : nothing
Base.getindex(sch::Schema, col::String) = sch.index[col]

function Base.show{R, T}(io::IO, schema::Schema{R, T})
    println(io, "Data.Schema:")
    println(io, "rows: $(schema.rows)\tcols: $(schema.cols)")
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
        newtypes = map(x->x >: Null ? ifelse(Nulls.T(x) <: WeakRefString, ?String, x) : ifelse(x <: WeakRefString, String, x), newtypes)
    end
    return Schema(Data.header(sch), newtypes, size(sch, 1), sch.metadata), transforms2
end
transform(sch::Data.Schema, transforms::Dict{String, <:Function}, s) = transform(sch, Dict{Int, Function}(sch[x]=>f for (x, f) in transforms), s)

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
    # println("Data.stream! from $So => $Si")
    sinkstreamtypes = Data.streamtypes(Si)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(So, sinkstreamtype)
            # println("can stream using $sinkstreamtype Data.StreamType")
            source_schema = Data.schema(source)
            wk = weakrefstrings(Si)
            sink_schema, transforms2 = Data.transform(source_schema, transforms, wk)
            # println("STREAMING: $source_schema => $sink_schema")
            # println("Constructing sink for streaming...")
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
    # println("Data.stream! from $So => $Si")
    sinkstreamtypes = Data.streamtypes(Si)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(So, sinkstreamtype)
            # println("can stream using $sinkstreamtype Data.StreamType")
            source_schema = Data.schema(source)
            wk = weakrefstrings(Si)
            sink_schema, transforms2 = transform(source_schema, transforms, wk)
            # println("streaming to existing sink...")
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
  # column selecting
  # RandomAccess
  # row filtering

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

Data.streamfrom(source, ::Type{Data.Column}, ::Type{T}, row::Int, col::Int) where {T} = Data.streamfrom(source, Data.Column, T, col)

@inline function streamto!{S, T, N}(sink, ::Type{S}, source, ::Type{T}, row, col::Type{Val{N}}, f, knownrows)
    val = Data.streamfrom(source, S, T, row, N)
    if val isa Null
        return Data.streamto!(sink, S, f(val), row, col, knownrows)
    else
        return Data.streamto!(sink, S, f(val), row, col, knownrows)
    end
end

function generate_loop(N, homogeneous, T, knownrows, S)
    if homogeneous
        # println("generating inner_loop w/ homogenous types...")
        inner_loop = quote
            for col = 1:$N
                val = Data.streamfrom(source, $S, $T, sinkrows + row, col)
                if val isa Null
                    Data.streamto!(sink, $S, transforms[col](val), sinkrows + row, col, $knownrows)
                else
                    Data.streamto!(sink, $S, transforms[col](val), sinkrows + row, col, $knownrows)
                end
            end
        end
    elseif N > 500
        # println("generating inner_loop w/ > 500 columns...")
        inner_loop = quote
            for col = 1:cols
                @inbounds Data.streamto!(sink, $S, source, sourcetypes[col], row, col, transforms[col], $knownrows)
            end
        end
    else
        # println("generating inner_loop w/ @nexprs...")
        inner_loop = quote
            Base.@nexprs $N col->begin
                val_col = Data.streamfrom(source, $S, sourcetypes[col], sinkrows + row, col)
                # hack to improve codegen due to inability of inference to inline Union{T, Null} val_col here
                if val_col isa Null
                    Data.streamto!(sink, $S, transforms[col](val_col), sinkrows + row, Val{col}, $knownrows)
                else
                    Data.streamto!(sink, $S, transforms[col](val_col), sinkrows + row, Val{col}, $knownrows)
                end
            end
        end
    end
    if knownrows == Val{true}
        # println("generating loop w/ known rows...")
        loop = quote
            for row = 1:rows
                $inner_loop
            end
        end
    else
        # println("generating loop w/ unknown rows...")
        loop = quote
            row = 1
            while true
                $inner_loop
                Data.isdone(source, row, cols) && break
                row += 1
            end
            Data.setrows!(source, row)
        end
    end
    # println(loop)
    return loop
end


@generated function Data.stream!{So, S<:StreamType, Si, R, T1}(source::So, ::Type{S}, sink::Si,
    source_schema::Schema{R, T1}, sink_schema, transforms, filter, columns)
    sourcetypes = Tuple(T1.parameters)
    homogeneous = all(i -> (T1.parameters[1] === i), T1.parameters)
    T = T1.parameters[1]
    N = length(sourcetypes)
    knownrows = R == Null ? Val{false} : Val{true}
    r = quote
        rows, cols = size(source_schema)::Tuple{$R, Int}
        Data.isdone(source, 1, 1) && return sink
        sinkrows = max(0, size(sink_schema, 1)::Int - (ifelse(rows isa Null, 0, rows)))
        sourcetypes = $sourcetypes
        try
            $(generate_loop(N, homogeneous, T, knownrows, S))
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
    # println(r)
    return r
end

# NamedTuple Data.Source
function Data.schema(df::NamedTuple{names, T}) where {names, T}
    return Data.Schema(collect(map(string, names)),
                       Type[eltype(A) for A in T.parameters], length(getfield(df, 1)))
end

# NamedTuple as a Data.Source
function Data.isdone(source::NamedTuple, row, col)
    rows, cols = size(source)
    return row > rows || col > cols
end

#FIXME: avoid the type piracy here
Base.getindex(n::NamedTuple, row, col) = getindex(getfield(n, col), row)
Base.size(n::NamedTuple{names}, dim) where {names} = dim == 1 ? length(getfield(n, names[1])) : length(names)

Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Column}) = true
Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Field}) = true

# Data.streamfrom{T <: AbstractVector}(source::NamedTuple, ::Type{Data.Column}, ::Type{T}, col) =
#     (A = source.columns[col]::T; return A)
Data.streamfrom{T}(source::NamedTuple, ::Type{Data.Column}, ::Type{T}, col) =
    (A = source.columns[col]::AbstractVector{T}; return A)
Data.streamfrom{T}(source::NamedTuple, ::Type{Data.Field}, ::Type{T}, row, col) =
    (A = source.columns[col]::AbstractVector{T}; return A[row]::T)

# NamedTuple Data.Sink
Data.streamtypes(::Type{<:NamedTuple}) = [Data.Column, Data.Field]
Data.weakrefstrings(::Type{<:NamedTuple}) = true

allocate{T}(::Type{T}, rows, ref) = Vector{T}(rows)
# allocate{T}(::Type{Vector{T}}, rows, ref) = Vector{T}(rows)
allocate(::Type{T}, rows, ref) where {T <: ?WeakRefString} = WeakRefStringArray(ref, T, rows)

function NamedTuple(sch::Data.Schema{R}, ::Type{S},
                    append::Bool, args...; reference::Vector{UInt8}=UInt8[]) where {R, S}
    types = Data.types(sch)
    if !isempty(args) && types == Data.types(Data.schema(args[1]))
        # passing in an existing NamedTuple Sink w/ same types
        sink = args[1]
        if append && (S <: Data.Column || R == Null)
            # don't need to do anything, just return existing sink
            # println("don't need to adjust sink")
        else
            # println("adjusting an existing sink")
            sinkrows = size(sink, 1)
            newsize = ifelse(S <: Data.Column || R == Null, 0, ifelse(append, sinkrows + sch.rows, sinkrows))
            # @show newsize
            foreach(col->resize!(col, newsize), sink)
            sch.rows = newsize
        end
        if !isempty(reference)
            foreach(col-> col isa WeakRefStringArray && push!(col.data, reference), sink)
        end
    else
        # println("allocating a fresh sink")
        # allocating a fresh NamedTuple Sink; append is irrelevant
        rows = ifelse(S <: Data.Column, 0, ifelse(R == Null, 0, sch.rows))
        columns = ((Symbol(x) for x in Data.header(sch))...)
        sink = Base.namedtuple(NamedTuple{columns}, (allocate(types[i], rows, reference) for i = 1:length(types))...)
    end
    # @show typeof(sink)
    return sink
end

function (::Type{N})(sink::N, sch::Data.Schema, ::Type{S}, append::Bool; reference::Vector{UInt8}=UInt8[]) where {N <: NamedTuple, S}
    return NamedTuple(sch, S, append, sink; reference=reference)
end

@inline Data.streamto!{names, C, T, N}(sink::NamedTuple{names, C}, ::Type{Data.Field}, val::T, row, col::Type{Val{N}}, ::Type{Val{false}}) =
    push!(getfield(sink, N), val)
@inline Data.streamto!{names, C, T, N}(sink::NamedTuple{names, C}, ::Type{Data.Field}, val::T, row, col::Type{Val{N}}, ::Type{Val{true}}) =
    getfield(sink, N)[row] = val

@inline Data.streamto!{names, C, T}(sink::NamedTuple{names, C}, ::Type{Data.Field}, val::T, row, col::Int, ::Type{Val{false}}) =
    push!(getfield(sink, col), val)
@inline Data.streamto!{names, C, T}(sink::NamedTuple{names, C}, ::Type{Data.Field}, val::T, row, col::Int, ::Type{Val{true}}) =
    getfield(sink, col)[row] = val

@inline function Data.streamto!{names, C, T, N}(sink::NamedTuple{names, C}, ::Type{Data.Column}, column::T, row, col::Type{Val{N}}, knownrows)
    append!(getfield(sink, N), column)
    return nothing
    # return length(column)
end
@inline function Data.streamto!{names, C, T}(sink::NamedTuple{names, C}, ::Type{Data.Column}, column::T, row, col::Int, knownrows)
    append!(getfield(sink, col), column)
    return nothing
    # return length(column)
end
end # module Data

end # module DataStreams
