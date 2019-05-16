# Source/Sink with NamedTuple, both row and column oriented
"A default row-oriented \"table\" that supports both the `Data.Source` and `Data.Sink` interfaces. Can be used like `Data.stream!(source, Data.RowTable)`. It is represented as a Vector of NamedTuples."
const RowTable{T} = Vector{T} where {T <: NamedTuple}
Data.isdone(source::RowTable, row, col, rows, cols) = row > rows || col > cols
function Data.isdone(source::RowTable, row, col)
    rows = length(source)
    return Data.isdone(source, row, col, rows, rows > 0 ? length(rows[1]) : 0)
end
Data.streamtype(::Type{Array}, ::Type{Data.Field}) = true
Data.streamfrom(source::RowTable, ::Type{Data.Field}, ::Type{T}, row, col) where {T} = source[row][col]
Data.streamtypes(::Type{Array}) = [Data.Row]
Data.accesspattern(::RowTable) = Data.RandomAccess()

Data.streamto!(sink::RowTable, ::Type{Data.Row}, val, row, col) =
    row > length(sink) ? push!(sink, val) : setindex!(sink, val, row)
Data.streamto!(sink::RowTable, ::Type{Data.Row}, val, row, col::Int, ::Type{Val{false}}) =
    push!(sink, val)
Data.streamto!(sink::RowTable, ::Type{Data.Row}, val, row, col::Int, ::Type{Val{true}}) =
    setindex!(sink, val, row)

# Basically, a NamedTuple with any # of AbstractVector elements, accessed by column name
"A default column-oriented \"table\" that supports both the `Data.Source` and `Data.Sink` interfaces. Can be used like `Data.stream!(source, Data.Table). It is represented as a NamedTuple of AbstractVectors.`"
const Table = NamedTuple{names, T} where {names, T <: NTuple{N, AbstractVector{S} where S}} where {N}

function Data.schema(rt::RowTable{NamedTuple{names, T}}) where {names, T}
    return Data.Schema(Type[A for A in T.parameters],
                        collect(map(string, names)), length(rt))
end
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
Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Column}) = true
Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Field}) = true
Data.accesspattern(::Table) = Data.RandomAccess()

# Data.streamfrom is pretty simple, just return the cell or column
@inline Data.streamfrom(source::Table, ::Type{Data.Column}, T, row::Integer, col::Integer) = source[col]
@inline Data.streamfrom(source::Table, ::Type{Data.Field}, T, row::Integer, col::Integer) = source[col][row]

# NamedTuple Data.Sink implementation
# we support both kinds of streaming to our type
Data.streamtypes(::Type{<:NamedTuple}) = [Data.Column, Data.Field]
# we support streaming WeakRefStrings
Data.weakrefstrings(::Type{<:NamedTuple}) = true

# convenience methods for "allocating" a single column for streaming
allocate(::Type{T}, rows, ref) where {T} = Vector{T}(undef, rows)
# allocate(::Type{T}, rows, ref) where {T <: Union{CategoricalValue, Missing}} =
#     CategoricalArray{CategoricalArrays.unwrap_catvalue_type(T)}(rows)
# special case for WeakRefStrings
allocate(::Type{WeakRefString{T}}, rows, ref) where {T} = StringVector{String}(ref, rows)
allocate(::Type{Union{WeakRefString{T}, Missing}}, rows, ref) where {T} = StringVector{String}(ref, rows)
allocate(::Type{Missing}, rows, ref) = fill(missing, rows)

# NamedTuple doesn't allow duplicate names, so make sure there are no duplicates in our column names
function makeunique(names::Vector{String})
    nms = [Symbol(nm) for nm in names]
    seen = Set{Symbol}()
    for (i, x) in enumerate(nms)
        x in seen ? setindex!(nms, Symbol("$(x)_$i"), i) : push!(seen, x)
    end
    return (nms...,)
end

function Array(sch::Data.Schema{R}, ::Type{Data.Row}, append::Bool=false, args...) where {R}
    types = Data.types(sch)
    # check if we're dealing with an existing NamedTuple sink or not
    if !isempty(args) && args[1] isa RowTable && types == Data.types(Data.schema(args[1]))
        sink = args[1]
        sinkrows = size(Data.schema(sink), 1)
        if append && !R
            sch.rows = sinkrows
        else
            newsize = ifelse(!R, 0, ifelse(append, sinkrows + sch.rows, sch.rows))
            resize!(sink, newsize)
            sch.rows = newsize
        end
    else
        rows = ifelse(!R, 0, sch.rows)
        names = makeunique(Data.header(sch))
        # @show rows, names, types
        sink = Vector{NamedTuple{names, Tuple{types...}}}(undef, rows)
        sch.rows = rows
    end
    return sink
end

function Array(sink::RowTable, sch::Data.Schema, ::Type{Data.Row}, append::Bool)
    return Array(sch, Data.Row, append, sink)
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
              # for Data.Field, the # of rows in the source are unknown (ismissing(rows)), so we'll just push! in streamto!
        else
            # need to adjust the existing sink
            # similar to above, for Data.Column or unknown # of rows for Data.Field, we'll append!/push!, so we empty! the columns
            # if appending, we want to grow our columns to be able to include every row in source (sinkrows + sch.rows)
            # if not appending, we're just "re-using" a sink, so we just resize it to the # of rows in the source
            newsize = ifelse(S == Data.Column || !R, 0, ifelse(append, sinkrows + sch.rows, sch.rows))
            foreach(col->resize!(col, newsize), sink)
            sch.rows = newsize
        end
        # take care of a possible reference from source by letting StringVector hold on to them
        if !isempty(reference)
            foreach(col-> col isa StringVector && append!(col.buffer, reference), sink)
        end
    else
        # allocating a fresh NamedTuple Sink; append is irrelevant
        # for Data.Column or unknown # of rows in Data.Field, we only ever append!, so just allocate empty columns
        rows = ifelse(S == Data.Column, 0, ifelse(!R, 0, sch.rows))
        names = makeunique(Data.header(sch))

        sink = NamedTuple{names}(Tuple(allocate(types[i], rows, reference) for i = 1:length(types)))
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
Data.streamto!(sink::Table, ::Type{Data.Field}, val, row, col::Int) =
    (A = getfield(sink, col); row > length(A) ? push!(A, val) : setindex!(A, val, row))
Data.streamto!(sink::Table, ::Type{Data.Field}, val, row, col::Int, ::Type{Val{false}}) =
    push!(getfield(sink, col), val)
Data.streamto!(sink::Table, ::Type{Data.Field}, val, row, col::Int, ::Type{Val{true}}) =
    getfield(sink, col)[row] = val
Data.streamto!(sink::Table, ::Type{Data.Column}, column, row, col::Int, knownrows) =
    append!(getfield(sink, col), column)


# Row iteration for Data.Sources
struct Rows{S, NT}
    source::S
end

Base.eltype(rows::Rows{S, NT}) where {S, NT} = NT
function Base.length(rows::Rows)
    sch = Data.schema(rows.source)
    return size(sch, 1)
end

"Returns a NamedTuple-iterator of any `Data.Source`"
function rows(source::S) where {S}
    sch = Data.schema(source)
    names = makeunique(Data.header(sch))
    types = Data.types(sch)
    return Rows{S, NamedTuple{names, Tuple{types...}}}(source)
end

function Base.iterate(rows::Rows{S, NamedTuple{names, types}}, row::Int=1) where {S, names, types}
    if @generated
        cols = length(names)
        vals = Tuple(:(Data.streamfrom(rows.source, Data.Field, $typ, row, $col)) for (col, typ) in zip(1:cols, types.parameters) )
        r = :(($(NamedTuple{names, types})(($(vals...),)), row + 1))
        # println(r)
        return quote
            Data.isdone(rows.source, row, $cols) && return nothing
            $r
        end
    else
        cols = length(names)
        return NamedTuple{names, types}(Tuple(Data.streamfrom(rows.source, Data.Field, T, row, i) for (i, T) in enumerate(types.parameters)))
    end
end
