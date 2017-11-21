if !isdefined(Core, :NamedTuple)
    using NamedTuples
end

# Source/Sink with NamedTuple, both row and column oriented
const RowTable{T} = Vector{T} where {T <: NamedTuple}
Data.isdone(source::RowTable, row, col, rows, cols) = row > rows || col > cols
function Data.isdone(source::RowTable, row, col)
    rows = length(source)
    return Data.isdone(source, row, col, rows, rows > 0 ? length(rows[1]) : 0)
end
Data.streamtype(::Type{Array}, ::Type{Data.Field}) = true
@inline Data.streamfrom(source::RowTable, ::Type{Data.Field}, ::Type{T}, row, col) where {T} = source[row][col]
Data.streamtypes(::Type{Array}) = [Data.Row]

@inline Data.streamto!(sink::RowTable, ::Type{Data.Row}, val, row, col) =
    row > length(sink) ? push!(sink, val) : setindex!(sink, val, row)
@inline Data.streamto!(sink::RowTable, ::Type{Data.Row}, val, row, col::Int, ::Type{Val{false}}) =
    push!(sink, val)
@inline Data.streamto!(sink::RowTable, ::Type{Data.Row}, val, row, col::Int, ::Type{Val{true}}) =
    setindex!(sink, val, row)

if isdefined(Core, :NamedTuple)

# Basically, a NamedTuple with any # of AbstractVector elements, accessed by column name
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

else # if isdefined(Core, :NamedTuple)

# Constraint relaxed for compatability; NamedTuples.NamedTuple does not have parameters
const Table = NamedTuple

function Data.schema(rt::RowTable{T}) where {T}
    return Data.Schema(Type[fieldtype(T, i) for i = 1:nfields(T)],
                        collect(map(string, fieldnames(T))), length(rt))
end
# NamedTuple Data.Source implementation
# compute Data.Schema on the fly
function Data.schema(df::NamedTuple)
    return Data.Schema(Type[eltype(A) for A in values(df)],
                        collect(map(string, keys(df))), length(df) == 0 ? 0 : length(getfield(df, 1)))
end

end # if isdefined(Core, :NamedTuple)

Data.isdone(source::Table, row, col, rows, cols) = row > rows || col > cols
function Data.isdone(source::Table, row, col)
    cols = length(source)
    return Data.isdone(source, row, col, cols == 0 ? 0 : length(getfield(source, 1)), cols)
end

# We support both kinds of streaming
Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Column}) = true
Data.streamtype(::Type{<:NamedTuple}, ::Type{Data.Field}) = true

# Data.streamfrom is pretty simple, just return the cell or column
@inline Data.streamfrom(source::Table, ::Type{Data.Column}, ::Type{T}, row, col) where {T} = source[col]
@inline Data.streamfrom(source::Table, ::Type{Data.Field}, ::Type{T}, row, col) where {T} = source[col][row]

# NamedTuple Data.Sink implementation
# we support both kinds of streaming to our type
Data.streamtypes(::Type{<:NamedTuple}) = [Data.Column, Data.Field]
# we support streaming WeakRefStrings
Data.weakrefstrings(::Type{<:NamedTuple}) = true

# convenience methods for "allocating" a single column for streaming
allocate(::Type{T}, rows, ref) where {T} = Vector{T}(rows)
# allocate(::Type{T}, rows, ref) where {T <: Union{CategoricalValue, Missing}} =
#     CategoricalArray{CategoricalArrays.unwrap_catvalue_type(T)}(rows)
# special case for WeakRefStrings
allocate(::Type{T}, rows, ref) where {T <: Union{WeakRefString,Missing}} = WeakRefStringArray(ref, T, rows)

# NamedTuple doesn't allow duplicate names, so make sure there are no duplicates in our column names
function makeunique(names::Vector{String})
    nms = [Symbol(nm) for nm in names]
    seen = Set{Symbol}()
    for (i, x) in enumerate(nms)
        x in seen ? setindex!(nms, Symbol("$(x)_$i"), i) : push!(seen, x)
    end
    return (nms...)
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
        sink = @static if isdefined(Core, :NamedTuple)
                Vector{NamedTuple{names, Tuple{types...}}}(rows)
            else
                exprs = [:($nm::$typ) for (nm, typ) in zip(names, types)]
                Vector{eval(NamedTuples.make_tuple(exprs))}(rows)
            end
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
        # take care of a possible reference from source by letting WeakRefStringArrays hold on to them
        if !isempty(reference)
            foreach(col-> col isa WeakRefStringArray && push!(col.data, reference), sink)
        end
    else
        # allocating a fresh NamedTuple Sink; append is irrelevant
        # for Data.Column or unknown # of rows in Data.Field, we only ever append!, so just allocate empty columns
        rows = ifelse(S == Data.Column, 0, ifelse(!R, 0, sch.rows))
        names = makeunique(Data.header(sch))

        sink = @static if isdefined(Core, :NamedTuple)
                NamedTuple{names}(Tuple(allocate(types[i], rows, reference) for i = 1:length(types)))
            else
                NamedTuples.make_tuple(collect(names))((allocate(types[i], rows, reference) for i = 1:length(types))...)
            end
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


# Row iteration for Data.Sources
struct Rows{S, NT}
    source::S
end

Base.eltype(rows::Rows{S, NT}) where {S, NT} = NT
function Base.length(rows::Rows)
    sch = Data.schema(rows.source)
    return size(sch, 1)
end

function rows(source::S) where {S}
    sch = Data.schema(source)
    names = makeunique(Data.header(sch))
    types = Data.types(sch)
    NT = @static if isdefined(Core, :NamedTuple)
            NamedTuple{names, Tuple{types...}}
        else
            exprs = [:($nm::$typ) for (nm, typ) in zip(names, types)]
            eval(NamedTuples.make_tuple(exprs))
        end
    return Rows{S, NT}(source)
end

Base.start(rows::Rows) = 1
@static if isdefined(Core, :NamedTuple)

@generated function Base.next(rows::Rows{S, NamedTuple{names, types}}, row::Int) where {S, names, types}
    vals = Tuple(:(Data.streamfrom(rows.source, Data.Field, $typ, row, $col)) for (col, typ) in zip(1:length(names), types.parameters) )
    r = :(($(NamedTuple{names, types})(($(vals...),)), row + 1))
    # println(r)
    return r
end
@generated function Base.done(rows::Rows{S, NamedTuple{names, types}}, row::Int) where {S, names, types}
    cols = length(names)
    return :(Data.isdone(rows.source, row, $cols))
end
else

@generated function Base.next(rows::Rows{S, NT}, row::Int) where {S, NT}
    names = fieldnames(NT)
    types = Tuple(fieldtype(NT, i) for i = 1:nfields(NT))
    vals = Tuple(:(Data.streamfrom(rows.source, Data.Field, $typ, row, $col)) for (col, typ) in zip(1:length(names), types) )
    r = :(($NT($(vals...)), row + 1))
    # println(r)
    return r
end
@generated function Base.done(rows::Rows{S, NT}, row::Int) where {S, NT}
    cols = length(fieldnames(NT))
    return :(Data.isdone(rows.source, row, $cols))
end

end
