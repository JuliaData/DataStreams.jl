struct Column{T, sourceindex, sinkindex, name}
end

struct SelectedColumn{C}
end

struct Query
    columns
    limit
    offset
end

# res = Data.query(df, [@NT(col=1,), @NT(col=2,), @NT(col=3,), @NT(col=4,)], sink)
function Query(types, header, acts, limit, offset)
    columns = []

    for act in acts
        col = act.col
        T = types[col]
        name = Symbol(get(act, :name, header[col]))
        push!(columns, SelectedColumn{Column{T, col, i, name}})
    end
    return Query(Tuple(columns), limit, offset)
end

struct QueryPlan
    pre_outer_loop::Expr
    starting_row
    streamfrom_inner_loop::Expr
    streamto_inner_loop::Expr
    rows
    post_outer_loop
end

QueryPlan() = QueryPlan(Expr(:block), 1, Expr(:block), Expr(:block), :row, Expr(:block))

function QueryPlan()

end

# answers the question: how do the rows/columns of Source get streamed according to query?
function generate_streamfrom(cols)
    sourceinds = sortperm(cols, by=x->sourceindex(x))
    sourcecolumns = [ind=>cols[ind] for ind in sourceinds]
    # rle: 
    #   * new element for change in types
    #   * 
end

function generate_loop(knownrows, S, code, collect(Any, columns.parameters), collect(Int, extras), collect(Any, sourcetypes), limit, offset)


    return quote
        $pre_outer_loop
        sourcerow = $starting_row
        sinkrow = 1
        cur_row = 1
        while true
            $streamfrom_inner_loop
            $streamto_inner_loop
            @label end_of_loop
            sourcerow += cur_row # will be 1 for Data.Field, length(val) for Data.Column
            sinkrow += cur_row
            Data.isdone(source, sourcerow, cols, $rows, cols) && break
        end
        Data.setrows!(source, sourcerow)
        $post_outer_loop
    end
end


function query(source, actions=[], sink::Type{Si}=Table, args...; append::Bool=false, limit::(Integer|Nothing)=nothing, offset::(Integer|Nothing)=nothing, kwargs...) where {Si}
    sch = Data.schema(source)
    types = Data.anytypes(sch)
    header = Data.header(sch)
    q = Query(types, header, Vector{Any}(actions), limit, offset)
    outsink = Data.stream!(source, q, sink, args...; append=append, kwargs...)
    return Data.close!(outsink)
end

function query(source, actions, sink::Si; append::Bool=false, limit::(Integer|Nothing)=nothing, offset::(Integer|Nothing)=nothing) where {Si}
    sch = Data.schema(source)
    types = Data.anytypes(sch)
    header = Data.header(sch)
    q = Query(types, header, Vector{Any}(actions), limit, offset)
    outsink = Data.stream!(source, q, sink; append=append)
    return Data.close!(outsink)
end

function schema(source, q::Query{columns, limit, offset}, wk=true) where {columns, limit, offset}
    types = Tuple(unwk(T(col), wk) for col in columns.parameters if selected(col))
    header = Tuple(String(name(col)) for col in columns.parameters if selected(col))
    off = have(offset) ? offset : 0
    rows = size(Data.schema(source), 1)
    rows = have(limit) ? min(limit, rows - off) : rows - off
    # rows = (scalarfiltered(code) | grouped(code)) ? missing : rows
    return Schema(types, header, rows)
end

gettransforms(sch, d::Dict{Int, <:Base.Callable}) = d
gettransforms(sch, d::Dict{String, F}) where {F <: Base.Callable} = Dict{Int, F}(sch[x]=>f for (x, f) in d)

function Data.stream!(source::So, ::Type{Si}, args...;
                        append::Bool=false,
                        transforms::Dict=Dict{Int, Function}(),
                        filter::Function=TRUE,
                        columns::Vector=[],
                        actions=[], limit=nothing, offset=nothing,
                        kwargs...) where {So, Si}
    if isempty(transforms)
        acts = Vector{Any}(actions)
    elseif isempty(actions)
        # exclude transform columns, add scalarcomputed transform column w/ same name
        sch = Data.schema(source)
        trns = gettransforms(sch, transforms)
        acts = Any[@NT(col=i) for i = 1:sch.cols if !haskey(trns, i)]
        names = Data.header(sch)
        for (col, f) in trns
            Base.insert!(acts, col, @NT(name=names[col], compute=f, computeargs=(col,)))
        end
    else
        throw(ArgumentError("`transforms` is deprecated, use only `actions` to specify column transformations"))
    end
    sch = Data.schema(source)
    types = Data.anytypes(sch)
    header = Data.header(sch)
    q = Query(types, header, acts, limit, offset)
    return Data.stream!(source, q, Si, args...; append=append, kwargs...)
end

function Data.stream!(source::So, sink::Si;
                        append::Bool=false,
                        transforms::Dict=Dict{Int, Function}(),
                        filter::Function=TRUE,
                        actions=[], limit=nothing, offset=nothing,
                        columns::Vector=[]) where {So, Si}
    if isempty(transforms)
        acts = Vector{Any}(actions)
    elseif isempty(actions)
        # exclude transform columns, add scalarcomputed transform column w/ same name
        sch = Data.schema(source)
        trns = gettransforms(sch, transforms)
        acts = Any[@NT(col=i) for i = 1:sch.cols if !haskey(trns, i)]
        names = Data.header(sch)
        for (col, f) in trns
            Base.insert!(acts, col, @NT(name=names[col], compute=f, computeargs=(col,)))
        end
    else
        throw(ArgumentError("`transforms` is deprecated, use only `actions` to specify column transformations"))
    end
    sch = Data.schema(source)
    types = Data.anytypes(sch)
    header = Data.header(sch)
    q = Query(types, header, acts, limit, offset)
    return Data.stream!(source, q, sink; append=append)
end

function Data.stream!(source::So, q::Query, ::Type{Si}, args...; append::Bool=false, kwargs...) where {So, Si}
    S = datatype(Si)
    sinkstreamtypes = Data.streamtypes(S)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(datatype(So), sinkstreamtype)
            wk = weakrefstrings(S)
            sourceschema = Data.schema(source)
            sinkschema = Data.schema(source, q, wk)
            if wk
                sink = S(sinkschema, sinkstreamtype, append, args...; reference=Data.reference(source), kwargs...)
            else
                sink = S(sinkschema, sinkstreamtype, append, args...; kwargs...)
            end
            sourcerows = size(sourceschema, 1)
            sinkrows = size(sinkschema, 1)
            sinkrowoffset = ifelse(append, ifelse(ismissing(sourcerows), sinkrows, max(0, sinkrows - sourcerows)), 0)
            return Data.stream!(source, q, sinkstreamtype, sink, sourceschema, sinkrowoffset)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

function Data.stream!(source::So, q::Query, sink::Si; append::Bool=false) where {So, Si}
    S = datatype(Si)
    sinkstreamtypes = Data.streamtypes(S)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(datatype(So), sinkstreamtype)
            wk = weakrefstrings(S)
            sourceschema = Data.schema(source)
            sinkschema = Data.schema(source, q, wk)
            if wk
                sink = S(sink, sinkschema, sinkstreamtype, append; reference=Data.reference(source))
            else
                sink = S(sink, sinkschema, sinkstreamtype, append)
            end
            sourcerows = size(sourceschema, 1)
            sinkrows = size(sinkschema, 1)
            sinkrowoffset = ifelse(append, ifelse(ismissing(sourcerows), sinkrows, max(0, sinkrows - sourcerows)), 0)
            return Data.stream!(source, q, sinkstreamtype, sink, sourceschema, sinkrowoffset)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

@generated function Data.stream!(source, q::Query{code, columns, extras, limit, offset}, ::Type{S}, sink,
                        sourceschema::Data.Schema{R, T1}, sinkrowoffset) where {S <: Data.StreamType, R, T1, code, columns, extras, limit, offset}
    types = T1.parameters
    sourcetypes = Tuple(types)
    # runlen = rle(sourcetypes)
    T = isempty(types) ? Any : types[1]
    homogeneous = all(i -> (T === i), types)
    N = length(types)
    knownrows = R && !scalarfiltered(code) && !grouped(code)
    RR = R ? Int : Missing
    @show knownrows, S, code, collect(Any, columns.parameters), collect(Int, extras), collect(Any, sourcetypes), limit, offset
    r = quote
        rows, cols = size(sourceschema)::Tuple{$RR, Int}
        Data.isdone(source, 1, 1, rows, cols) && return sink
        sourcetypes = $sourcetypes
        N = $N
        try
            $(generate_loop(knownrows, S, code, collect(Any, columns.parameters), collect(Int, extras), collect(Any, sourcetypes), limit, offset))
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
    # @show columns
    println(expr(r))
    return r
end

function expr(ex)
    todelete = Int[]
    for (i, arg) in enumerate(ex.args)
        if typeof(arg) <: LineNumberNode
            push!(todelete, i)
        elseif typeof(arg) <: Expr
            expr(arg)
        end
    end
    deleteat!(ex.args, todelete)
    return ex
end
