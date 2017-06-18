__precompile__(true)
module DataStreams

export Data

module Data

using Compat, Nulls, WeakRefStrings

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
 * `Data.Schema(types[, rows, meta::Dict])`: column element types are provided as a tuple or vector; # of rows can be an Int or `null` to indicate unknown # of rows
 * `Data.Schema(header, types[, rows, meta])`: provide explicit column names in addition to column element types

`Data.Schema` are indexable via column names to get the number of that column in the `Data.Schema`

```julia
julia> sch = Data.Schema(["column1"], [Int], 10)
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
    rows::(?Int)             # number of rows in the dataset
    cols::Int                # number of columns in a dataset
    metadata::Dict           # for any other metadata we'd like to keep around (not used for '==' operation)
    index::Dict{String, Int} # maps column names as Strings to their index # in `header` and `types`
end

function Schema(types=(), header=String["Column$i" for i = 1:length(types)], rows::(?Integer)=0, metadata::Dict=Dict())
    !isnull(rows) && rows < 0 && throw(ArgumentError("Invalid # of rows for Data.Schema; use `null` to indicate an unknown # of rows"))
    types2 = Tuple(types)
    header2 = String[string(x) for x in header]
    cols = length(header2)
    cols != length(types2) && throw(ArgumentError("length(header): $(length(header2)) must == length(types): $(length(types2))"))
    return Schema{rows != null, Tuple{types2...}}(header2, rows, cols, metadata, Dict(n=>i for (i, n) in enumerate(header2)))
end

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
        newtypes = map(x->x >: Null ? ifelse(Nulls.T(x) <: WeakRefString, ?String, x) : ifelse(x <: WeakRefString, String, x), newtypes)
    end
    return Schema(newtypes, Data.header(sch), size(sch, 1), sch.metadata), transforms2
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
"""
Data.Source types must at least implement:

Data.isdone(source::S, row::Int, col::Int)

If more convenient/performant, they can also implement:

Data.isdone(source::S, row::Int, col::Int, rows::Int, cols::Int)

where `rows` and `cols` are the size of the `source` passed in.
"""
function isdone end
isdone(source, row, col, rows, cols) = isdone(source, row, col)

"""
`Data.streamtype{T<:Data.Source, S<:Data.StreamType}(::Type{T}, ::Type{S})` => Bool

Indicates whether the source `T` supports streaming of type `S`. To be overloaded by individual sources according to supported `Data.StreamType`s
"""
function streamtype end

"""
Data.reset!(source) resets a source into a state that allows streaming its data again.
"""
function reset! end

"""
Data.Sink types must implement one of the following:

Data.streamfrom(source, ::Type{Data.Column}, ::Type{T}, row::Int, col::Type{Val{N}}) where {T, N}
Data.streamfrom(source, ::Type{Data.Column}, ::Type{T}, row::Int, col::Int) where {T}
Data.streamfrom(source, ::Type{Data.Column}, ::Type{T}, col::Int) where {T}
"""
function streamfrom end
Data.streamfrom(source, ::Type{Data.Column}, T, row, col) = Data.streamfrom(source, Data.Column, T, col)

# Generic fallbacks
Data.streamtype(source, ::Type{<:StreamType}) = false
Data.reset!(source) = nothing

struct RandomAccess end
struct Sequential end
accesspattern(x) = Sequential()

const EMPTY_REFERENCE = UInt8[]
"""
Data.Source types can optionally implement

Data.reference(x::Source) => Vector{UInt8}

where the type retruns a `Vector{UInt8}` that represents a memory block that should be kept in reference for WeakRefStringArrays.
"""
reference(x) = EMPTY_REFERENCE

# Data.Sink Interface
@compat abstract type Sink end

# Required methods
# Sink(sch::Data.Schema, S, append, args...; reference::Vector{UInt8}=UInt8[], kwargs...)
# Sink(sink, sch::Data.Schema, S, append; reference::Vector{UInt8}=UInt8[], )
"""
`Data.streamtypes{T<:Data.Sink}(::Type{T})` => Vector{StreamType}

Returns a list of `Data.StreamType`s that the sink is able to receive; the order of elements indicates the sink's streaming preference
"""
function streamtypes end

function streamto! end
Data.streamto!(sink, S, val, row, col, knownrows) = Data.streamto!(sink, S, val, row, col)
Data.streamto!(sink, S, val, row, col) = Data.streamto!(sink, S, val, col)

# Optional methods
function cleanup! end
function close! end

# Generic fallbacks
cleanup!(sink) = nothing
close!(sink) = sink
weakrefstrings(x) = false

# Data.stream!
function stream! end

# generic public definitions
const TRUE = x->true
# the 2 methods below are safe and expected to be called from higher-level package convenience functions (e.g. CSV.read)
function Data.stream!(source::So, ::Type{Si}, args...;
                        append::Bool=false,
                        transforms::Dict=Dict{Int, Function}(),
                        filter::Function=TRUE,
                        columns::Vector=[],
                        kwargs...) where {So, Si}
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
    println(macroexpand(loop))
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

const Table = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}

# NamedTuple Data.Source
function Data.schema(df::NamedTuple{names, T}) where {names, T}
    return Data.Schema(Type[eltype(A) for A in T.parameters],
                        collect(map(string, names)), length(df) == 0 ? 0 : length(getfield(df, 1)))
end

# NamedTuple as a Data.Source
Data.isdone(source::Table, row, col, rows, cols) = row > rows || col > cols
function Data.isdone(source::Table, row, col)
    cols = length(source)
    return Data.isdone(source, row, col, cols == 0 ? 0 : length(getfield(source, 1)), cols)
end

Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Column}) = true
Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Field}) = true

@inline Data.streamfrom(source::Table, ::Type{Data.Column}, ::Type{T}, row, col) where {T} = source[col]
@inline Data.streamfrom(source::Table, ::Type{Data.Field}, ::Type{T}, row, col) where {T} = source[col][row]

# NamedTuple Data.Sink
Data.streamtypes(::Type{<:NamedTuple}) = [Data.Column, Data.Field]
Data.weakrefstrings(::Type{<:NamedTuple}) = true

allocate(::Type{T}, rows, ref) where {T} = Vector{T}(rows)
allocate(::Type{T}, rows, ref) where {T <: ?WeakRefString} = WeakRefStringArray(ref, T, rows)

# NamedTuple doesn't allow duplicate names, so make sure there are no duplicates
function makeunique(names::Vector{String})
    nms = Vector{Symbol}(names)
    seen = Set{Symbol}()
    for (i, x) in enumerate(nms)
        x in seen ? setindex!(nms, Symbol("$(x)_$i"), i) : push!(seen, x)
    end
    return (nms...)
end

function NamedTuple(sch::Data.Schema{R}, ::Type{S}=Data.Field,
                    append::Bool=false, args...; reference::Vector{UInt8}=UInt8[]) where {R, S <: StreamType}
    types = Data.types(sch)
    if !isempty(args) && args[1] isa Table && types == Data.types(Data.schema(args[1]))
        # passing in an existing NamedTuple Sink w/ same types
        sink = args[1]
        sinkrows = size(Data.schema(sink), 1)
        if append && (S <: Data.Column || !R)
            sch.rows = sinkrows
            # dont' need to do anything because:
              # for Data.Column, we just append columns anyway (see Data.streamto! below)
              # for Data.Field, the # of rows in the source are unknown (isnull(rows)), so we'll just push! in streamto!
        else
            # need to adjust the existing sink
            # similar to above, for Data.Column or unknown # of rows for Data.Field, we'll append!/push!, so we empty! the columns
            # if appending, we want to grow our columns to be able to include every row in source (sinkrows + sch.rows)
            # if not appending, we're just "re-using" a sink, so we just resize it to the # of rows in the source
            newsize = ifelse(S <: Data.Column || !R, 0, ifelse(append, sinkrows + sch.rows, sch.rows))
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
        rows = ifelse(S <: Data.Column, 0, ifelse(!R, 0, sch.rows))
        names = makeunique(Data.header(sch))
        sink = Base.namedtuple(NamedTuple{names}, (allocate(types[i], rows, reference) for i = 1:length(types))...)
        sch.rows = rows
    end
    return sink
end

function (::Type{N})(sink::N, sch::Data.Schema, ::Type{S}, append::Bool; reference::Vector{UInt8}=UInt8[]) where {N <: NamedTuple, S}
    return NamedTuple(sch, S, append, sink; reference=reference)
end

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