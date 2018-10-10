include("queryutils.jl")

"""
Represents a column used in a Data.Query for querying a Data.Source

Passed as the `actions` argument as an array of NamedTuples to `Data.query(source, actions, sink)`

Options include:

  * `col::Integer`: reference to a source column index
  * `name`: the name the column should have in the resulting query, if none is provided, it will be inferred from the `header` and `col` arguments or auto-generated
  * `T`: the type of the column, if not provided, it will be inferred from the `types` and `col` arguments
  * `hide::Bool`: whether the column should be shown in the query resultset; `hide=false` is useful for columns used only for filtering and not needed in the final resultset
  * `filter::Function`: a function to apply to this column to filter out rows where the result is `false`
  * `having::Function`: a function to apply to an aggregated column to filter out rows after applying an aggregation function
  * `compute::Function`: a function to generate a new column, requires a tuple of column indexes `computeargs` that correspond to the function inputs
  * `computeaggregate::Function`: a function to generate a new aggregated column, requires a tuple of column indexes `computeargs` that correspond to the function inputs
  * `computeargs::NTuple{N, Int}`: tuple of column indexes to indicate which columns should be used as inputs to a `compute` or `computeaggregate` function
  * `sort::Bool`: whether this column should be sorted; default `false`
  * `sortindex::Intger`: by default, a resultset will be sorted by sorted columns in the order they appear in the resultset; `sortindex` allows overriding to indicate a custom sorting order
  * `sortasc::Bool`: if a column is `sort=true`, whether it should be sorted in ascending order; default `true`
  * `group::Bool`: whether this column should be grouped, causing other columns to be aggregated
  * `aggregate::Function`: a function to reduce a columns values based on grouping keys, should be of the form `f(A::AbstractArray) => scalar`
"""
struct QueryColumn{code, T, sourceindex, sinkindex, name, sort, args}
    filter::(Function|Nothing)
    having::(Function|Nothing)
    compute::(Function|Nothing)
    aggregate::(Function|Nothing)
end

function QueryColumn(sourceindex::Integer, types=[], header=String[];
                name=Symbol(""),
                T::Type=Any,
                sinkindex::(Integer|Nothing)=sourceindex,
                hide::Bool=false,
                filter::(Function|Nothing)=nothing,
                having::(Function|Nothing)=nothing,
                compute::(Function|Nothing)=nothing,
                computeaggregate::(Function|Nothing)=nothing,
                computeargs=nothing,
                sort::Bool=false,
                sortindex::(Integer|Nothing)=nothing,
                sortasc::Bool=true,
                group::Bool=false,
                aggregate::(Function|Nothing)=nothing,
                kwargs...)
    # validate
    have(compute) && have(computeaggregate) && throw(ArgumentError("column can't be computed as scalar & aggregate"))
    (have(compute) || have(computeaggregate)) && !have(computeargs) && throw(ArgumentError("must provide computeargs=(x, y, z) to specify column index arguments for compute function"))
    have(filter) && have(computeaggregate) && throw(ArgumentError("column can't apply scalar filter & be aggregate computed"))
    group && have(having) && throw(ArgumentError("can't apply having filter on grouping column, use scalar `filter=func` instead"))
    group && have(computeaggregate) && throw(ArgumentError("column can't be part of grouping and aggregate computed"))
    group && have(aggregate) && throw(ArgumentError("column can't be part of grouping and aggregated"))
    group && hide && throw(ArgumentError("grouped column must be included in resultset"))
    sort && !have(sortindex) && throw(ArgumentError("must provide sortindex if column is sorted"))
    sort && hide && throw(ArgumentError("sorted column must be included in resultset"))

    args = ()
    code = UNUSED
    for (arg, c) in (!hide=>SELECTED, sort=>SORTED, group=>GROUPED)
        arg && (code |= c)
    end
    for (arg, c) in ((filter, SCALARFILTERED),
                     (having, AGGFILTERED),
                     (compute, SCALARCOMPUTED),
                     (computeaggregate, AGGCOMPUTED))
        have(arg) && (code |= c)
    end
    T = (T == Any && length(types) >= sourceindex) ? types[sourceindex] : T
    name = name == Symbol("") && length(header) >= sourceindex ? Symbol(header[sourceindex]) : Symbol(name)
    computefn = nothing
    if have(compute) || have(computeaggregate)
        args = computeargs
        computefn = have(compute) ? compute : computeaggregate
        T = return_type(computefn, have(compute) ? tuplesubset(types, args) : Tuple(Vector{T} for T in tuplesubset(types, args)))
        name = name == Symbol("") ? Symbol("Column$sinkindex") : Symbol(name)
    elseif have(aggregate)
        T = return_type(aggregate, (Vector{T},))
    end
    S = sort ? Sort{sortindex, sortasc} : nothing
    return QueryColumn{code, T, sourceindex, sinkindex, name, S, args}(filter, having, computefn, aggregate)
end

for (f, c) in (:selected=>SELECTED,
               :scalarfiltered=>SCALARFILTERED,
               :aggfiltered=>AGGFILTERED,
               :scalarcomputed=>SCALARCOMPUTED,
               :aggcomputed=>AGGCOMPUTED,
               :sorted=>SORTED,
               :grouped=>GROUPED)
    @eval $f(code::QueryCodeType) = (code & $c) > 0
    @eval $f(x) = $f(code(x))
    @eval $f(x::QueryColumn{code}) where {code} = $f(code)
end

for (f, arg) in (:code=>:c, :T=>:t, :sourceindex=>:so, :sinkindex=>:si, :name=>:n, :sort=>:s, :args=>:a)
    @eval $f(::Type{<:QueryColumn{c, t, so, si, n, s, a}}) where {c, t, so, si, n, s, a} = $arg
    @eval $f(::QueryColumn{c, t, so, si, n, s, a}) where {c, t, so, si, n, s, a} = $arg
    @eval $f(::Nothing) = missing
end

# E type parameter is for a tuple of integers corresponding to
# column index inputs for aggcomputed columns
struct Query{code, T, E, L, O}
    columns::T # Tuple{QueryColumn...}, columns are in *output* order (i.e. monotonically increasing by sinkindex)
end

function Query(types::Vector{Any}, header::Vector{String}, actions::Vector{Any}, limit=nothing, offset=nothing)
    len = length(types)
    outlen = length(types)
    columns = []
    cols = Set()
    extras = Set()
    aggcompute_extras = Set()
    si = 0
    outcol = 1
    isempty(actions) && (actions = [(col=i,) for i = 1:len])
    for x in actions
        # if not provided, set sort index order according to order columns are given
        sortindex = get(x, :sortindex) do
            sorted = get(x, :sort, false)
            if sorted
                si += 1
                return si
            else
                return nothing
            end
        end
        if get(x, :hide, false)
            sinkindex = outlen + 1
            outlen += 1
        else
            sinkindex = outcol
            outcol += 1
        end
        foreach(i->i in cols || push!(extras, i), get(x, :computeargs, ()))
        push!(columns, QueryColumn(
                        get(()->(len += 1; return len), x, :col),
                        types, header;
                        sinkindex=sinkindex,
                        sortindex=sortindex,
                        ((k, getfield(x, k)) for k in keys(x))...)
        )
        push!(cols, get(x, :col, 0))
        if aggcomputed(typeof(columns[end]))
            foreach(i->push!(aggcompute_extras, i), args(columns[end]))
        end
    end
    querycode = UNUSED
    for col in columns
        querycode |= code(typeof(col))
    end
    if grouped(querycode)
        for col in columns
            c = code(typeof(col))
            (grouped(c) || have(col.aggregate) || aggcomputed(c) || scalarfiltered(c)) ||
                throw(ArgumentError("in query with grouped columns, each column must be grouped or aggregated: " * string(col)))
        end
    end
    append!(columns, QueryColumn(x, types, header; hide=true, sinkindex=outlen+i) for (i, x) in enumerate(Base.sort(collect(extras))))
    columns = Tuple(columns)

    return Query{querycode, typeof(columns), Tuple(aggcompute_extras), limit, offset}(columns)
end

"""
    Data.query(source, actions, sink=Data.Table, args...; append::Bool=false, limit=nothing, offset=nothing)

Query a valid DataStreams `Data.Source` according to query `actions` and stream the result into `sink`.
`limit` restricts the total number of rows streamed out, while `offset` will skip initial N rows.
`append=true` will cause the `sink` to _accumulate_ the additional query resultset rows instead of replacing any existing rows in the sink.

`actions` is an array of NamedTuples, w/ each NamedTuple including one or more of the following query arguments:

  * `col::Integer`: reference to a source column index
  * `name`: the name the column should have in the resulting query, if none is provided, it will be inferred from the `header` and `col` arguments or auto-generated
  * `T`: the type of the column, if not provided, it will be inferred from the `types` and `col` arguments
  * `hide::Bool`: whether the column should be shown in the query resultset; `hide=false` is useful for columns used only for filtering and not needed in the final resultset
  * `filter::Function`: a function to apply to this column to filter out rows where the result is `false`
  * `having::Function`: a function to apply to an aggregated column to filter out rows after applying an aggregation function
  * `compute::Function`: a function to generate a new column, requires a tuple of column indexes `computeargs` that correspond to the function inputs
  * `computeaggregate::Function`: a function to generate a new aggregated column, requires a tuple of column indexes `computeargs` that correspond to the function inputs
  * `computeargs::NTuple{N, Int}`: tuple of column indexes to indicate which columns should be used as inputs to a `compute` or `computeaggregate` function
  * `sort::Bool`: whether this column should be sorted; default `false`
  * `sortindex::Intger`: by default, a resultset will be sorted by sorted columns in the order they appear in the resultset; `sortindex` allows overriding to indicate a custom sorting order
  * `sortasc::Bool`: if a column is `sort=true`, whether it should be sorted in ascending order; default `true`
  * `group::Bool`: whether this column should be grouped, causing other columns to be aggregated
  * `aggregate::Function`: a function to reduce a columns values based on grouping keys, should be of the form `f(A::AbstractArray) => scalar`
"""
function query end

function query(source, actions=[], sink::Type{Si}=Table, args...; append::Bool=false, limit::(Integer|Nothing)=nothing, offset::(Integer|Nothing)=nothing, kwargs...) where {Si}
    sch = Data.schema(source)
    types = Data.anytypes(sch, weakrefstrings(Si))
    header = Data.header(sch)
    q = Query(types, header, Vector{Any}(actions), limit, offset)
    outsink = Data.stream!(source, q, sink, args...; append=append, kwargs...)
    return Data.close!(outsink)
end

function query(source, actions, sink::Si; append::Bool=false, limit::(Integer|Nothing)=nothing, offset::(Integer|Nothing)=nothing) where {Si}
    sch = Data.schema(source)
    types = Data.anytypes(sch, weakrefstrings(Si))
    header = Data.header(sch)
    q = Query(types, header, Vector{Any}(actions), limit, offset)
    outsink = Data.stream!(source, q, sink; append=append)
    return Data.close!(outsink)
end

unwk(T, wk) = T
unwk(::Type{WeakRefString{T}}, wk) where {T} = wk ? WeakRefString{T} : String
unwk(::Type{Union{Missing,WeakRefString{T}}}, wk) where {T} = wk ? Union{Missing,WeakRefString{T}} : Union{Missing,String}

"Compute the Data.Schema of the resultset of executing Data.Query `q` against its source"
function schema(source::S, q::Query{c, columns, e, limit, offset}, wk=true) where {c, S, columns, e, limit, offset}
    types = Tuple(unwk(T(col), wk) for col in columns.parameters if selected(col))
    header = Tuple(String(name(col)) for col in columns.parameters if selected(col))
    off = have(offset) ? offset : 0
    rows = size(Data.schema(source), 1)
    rows = have(limit) ? min(limit, rows - off) : rows - off
    rows = (scalarfiltered(c) | grouped(c)) ? missing : rows
    return Schema(types, header, rows)
end

codeblock() = Expr(:block)
macro vals(ex)
    return esc(:(Symbol(string("vals", $ex))))
end
macro val(ex)
    return esc(:(Symbol(string("val", $ex))))
end

# generate the entire streaming loop, according to any QueryColumns passed by the user
function generate_loop(knownrows::Bool, S::DataType, code::QueryCodeType, cols::Vector{Any}, extras::Vector{Int}, sourcetypes, limit, offset)
    streamfrom_inner_loop = codeblock()
    streamto_inner_loop = codeblock()
    pre_outer_loop = codeblock()
    post_outer_loop = codeblock()
    post_outer_loop_streaming = codeblock()
    post_outer_loop_row_streaming_inner_loop = codeblock()
    aggregation_loop = codeblock()
    pre_aggregation_loop = codeblock()
    aggregation_inner_loop = codeblock()
    post_aggregation_loop = codeblock()
    aggregationkeys = []
    aggregationvalues = []
    aggregationcomputed = []
    aggregationfiltered = []
    sortcols = []
    sortbuffers = []
    selectedcols = []
    firstcol = nothing
    firstfilter = true
    colind = 1
    sourceinds = sortperm(cols, by=x->sourceindex(x))
    sourcecolumns = [ind=>cols[ind] for ind in sourceinds]
    SF = S == Data.Row ? Data.Field : S
    starting_row = 1
    if have(offset)
        starting_row = offset + 1
        push!(pre_outer_loop.args, :(Data.skiprows!(source, $S, 1, $offset)))
    end
    rows = have(limit) ? :(min(rows, $(starting_row + limit - 1))) : :rows

    # loop thru sourcecolumns first, to ensure we stream everything we need from the Data.Source
    for (ind, col) in sourcecolumns
        si = sourceindex(col)
        out = sinkindex(col)
        if out == 1
            # keeping track of the first streamed column is handy later
            firstcol = col
        end
        # streamfrom_inner_loop
        # we can skip any columns that aren't needed in the resultset; this works because the `sourcecolumns` are in sourceindex order
        while colind < sourceindex(col)
            push!(streamfrom_inner_loop.args, :(Data.skipfield!(source, $SF, $(sourcetypes[colind]), sourcerow, $colind)))
            colind += 1
        end
        colind += 1
        if scalarcomputed(col)
            # if the column is scalarcomputed, there's no streamfrom, we calculate from previously streamed values and the columns' `args`
            # this works because scalarcomputed columns are sorted last in `columns`
            computeargs = Tuple((@val c) for c in args(col))
            push!(streamfrom_inner_loop.args, :($(@val si) = calculate(q.columns[$ind].compute, $(computeargs...))))
        elseif !aggcomputed(col)
            # otherwise, if the column isn't aggcomputed, we just streamfrom
            r = (S == Data.Column && (have(offset) || have(limit))) ? :(sourcerow:$rows) : :sourcerow
            push!(streamfrom_inner_loop.args, :($(@val si) = Data.streamfrom(source, $SF, $(T(col)), $r, $(sourceindex(col)))))
        end
        if scalarfiltered(col)
            if S != Data.Column
                push!(streamfrom_inner_loop.args, quote
                    # in the scalar filtering case, we check this value immediately and if false,
                    # we can skip streaming the rest of the row
                    ff = filter(q.columns[$ind].filter, $(@val si))
                    if !ff
                        Data.skiprow!(source, $SF, sourcerow, $(sourceindex(col) + 1))
                        @goto end_of_loop
                    end
                end)
            else
                # Data.Column streaming means we need to accumulate row filters in a `filtered`
                # Bool array and column values will be indexed by this Bool array later
                if firstfilter
                    push!(streamfrom_inner_loop.args, :(filtered = fill(true, length($(@val si)))))
                    firstfilter = false
                end
                push!(streamfrom_inner_loop.args, :(filter(filtered, q.columns[$ind].filter, $(@val si))))
            end
        end
    end
    # streamfrom_inner_loop
    if S == Data.Column
        push!(streamfrom_inner_loop.args, :(cur_row = length($(@val sourceindex(firstcol)))))
    end
    # now we loop through query result columns, to build up code blocks for streaming to Data.Sink
    for (ind, col) in enumerate(cols)
        si = sourceindex(col)
        out = sinkindex(col)
        # streamto_inner_loop
        if S == Data.Row
            selected(col) && push!(selectedcols, col)
        end
        if !grouped(code)
            if sorted(code)
                # if we're sorted, then we temporarily buffer all values while streaming in
                if selected(col)
                    if S == Data.Column && scalarfiltered(code)
                        push!(streamto_inner_loop.args, :(concat!($(@vals out), $(@val si)[filtered])))
                    else
                        push!(streamto_inner_loop.args, :(concat!($(@vals out), $(@val si))))
                    end
                end
            else
                # if we're not sorting or grouping, we can just stream out in the inner loop
                if selected(col)
                    if S != Data.Row
                        if S == Data.Column && scalarfiltered(code)
                            push!(streamto_inner_loop.args, :(Data.streamto!(sink, $S, $(@val si)[filtered], sinkrowoffset + sinkrow, $out, Val{$knownrows})))
                        else
                            push!(streamto_inner_loop.args, :(Data.streamto!(sink, $S, $(@val si), sinkrowoffset + sinkrow, $out, Val{$knownrows})))
                        end
                    end
                end
            end
        end
        # aggregation_loop
        if grouped(col)
            push!(aggregationkeys, col)
            push!(pre_aggregation_loop.args, :($(@vals out) = Vector{$(T(col))}(undef, length(aggregates))))
            push!(aggregation_inner_loop.args, :($(@vals out)[i] = k[$(length(aggregationkeys))]))
        elseif !aggcomputed(col) && (selected(col) || sourceindex(col) in extras)
            push!(aggregationvalues, col)
            push!(pre_aggregation_loop.args, :($(@vals out) = Vector{$(T(col))}(undef, length(aggregates))))
            if selected(col)
                push!(aggregation_inner_loop.args, :($(@vals out)[i] = q.columns[$ind].aggregate(v[$(length(aggregationvalues))])))
            end
        elseif aggcomputed(col)
            push!(aggregationcomputed, ind=>col)
            push!(pre_aggregation_loop.args, :($(@vals out) = Vector{$(T(col))}(undef, length(aggregates))))
        end
        if aggfiltered(col)
            push!(aggregationfiltered, col)
            push!(post_aggregation_loop.args, :(filter(filtered, q.columns[$ind].having, $(@vals out))))
        end
        if sorted(code)
            selected(col) && !aggcomputed(col) && push!(sortbuffers, col)
        end
        if sorted(col)
            push!(sortcols, col)
        end
        # post_outer_loop_streaming
        if sorted(code) || grouped(code)
            if selected(col)
                if sorted(code) && aggfiltered(code)
                    push!(post_outer_loop_streaming.args, :($(@vals out) = $(@vals out)[filtered][sortinds]))
                elseif sorted(code)
                    push!(post_outer_loop_streaming.args, :($(@vals out) = $(@vals out)[sortinds]))
                elseif aggfiltered(code)
                    push!(post_outer_loop_streaming.args, :($(@vals out) = $(@vals out)[filtered]))
                end
                if S == Data.Column
                    push!(post_outer_loop_streaming.args, :(Data.streamto!(sink, $S, $(@vals out), sinkrowoffset + sinkrow, $out, Val{$knownrows})))
                elseif S == Data.Field
                    push!(post_outer_loop_row_streaming_inner_loop.args, :(Data.streamto!(sink, $S, $(@vals out)[row], sinkrowoffset + row, $out, Val{$knownrows})))
                end
            end
        end
    end
    # pre_outer_loop
    if grouped(code)
        K = Tuple{(T(x) for x in aggregationkeys)...}
        V = Tuple{(Vector{T(x)} for x in aggregationvalues)...}
        push!(pre_outer_loop.args, :(aggregates = Dict{$K, $V}()))
        if S == Data.Column && scalarfiltered(code)
            aggkeys =   Tuple(:($(@val sourceindex(col))[filtered]) for col in aggregationkeys)
            aggvalues = Tuple(:($(@val sourceindex(col))[filtered]) for col in aggregationvalues)
        else
            aggkeys =   Tuple(:($(@val sourceindex(col))) for col in aggregationkeys)
            aggvalues = Tuple(:($(@val sourceindex(col))) for col in aggregationvalues)
        end
        # collect aggregate key value(s) and add entry(s) to aggregates dict
        push!(streamto_inner_loop.args, :(aggregate(aggregates, ($(aggkeys...),), ($(aggvalues...),))))
        # push!(streamto_inner_loop.args, :(@show aggregates))
    elseif sorted(code)
        append!(pre_outer_loop.args, :($(@vals sinkindex(col)) = $(T(col))[]) for col in sortbuffers)
    end
    # aggregation_loop
    if grouped(code)
        for (ind, col) in aggregationcomputed
            valueargs = Tuple(:(v[$(findfirst(x->sourceindex(x) == i, aggregationvalues))]) for i in args(col))
            push!(aggregation_inner_loop.args, :($(@vals sinkindex(col))[i] = q.columns[$ind].compute($(valueargs...))))
        end
        if aggfiltered(code)
            pushfirst!(post_aggregation_loop.args, :(filtered = fill(true, length(aggregates))))
        end
        aggregation_loop = quote
            $pre_aggregation_loop
            for (i, (k, v)) in enumerate(aggregates)
                $aggregation_inner_loop
            end
            $post_aggregation_loop
        end
    end
    # post_outer_loop
    push!(post_outer_loop.args, aggregation_loop)
    if sorted(code)
        sort!(sortcols, by=x->sortind(sort(x)))
        if aggfiltered(code)
            push!(post_outer_loop.args, :(sortinds = fill(0, sum(filtered))))
            sortkeys = Tuple(:($(@vals sinkindex(x))[filtered]=>$(sortasc(sort(x)))) for x in sortcols)
        else
            push!(post_outer_loop.args, :(sortinds = fill(0, length($(@vals sinkindex(firstcol))))))
            sortkeys = Tuple(:($(@vals sinkindex(x))=>$(sortasc(sort(x)))) for x in sortcols)
        end
        push!(post_outer_loop.args, :(sort(sortinds, ($(sortkeys...),))))
    end
    push!(post_outer_loop.args, post_outer_loop_streaming)
    # Data.Row streaming out
    if sorted(code) || grouped(code)
        if S == Data.Field || S == Data.Row
            if S == Data.Row
                # post_outer_loop_row_streaming_inner_loop
                names = Tuple(name(x) for x in selectedcols)
                types = Tuple{(T(x) for x in selectedcols)...}
                inds = Tuple(:($(@vals sinkindex(x))[row]) for x in selectedcols)
                vals = :(vals = NamedTuple{$names, $types}(($(inds...),)))
                push!(post_outer_loop_row_streaming_inner_loop.args,
                    :(Data.streamto!(sink, Data.Row, $vals, sinkrowoffset + row, 0, Val{$knownrows})))
            end
            push!(post_outer_loop.args, quote
                for row = 1:length($(@vals sinkindex(firstcol)))
                    $post_outer_loop_row_streaming_inner_loop
                end
            end)
        end
    elseif S == Data.Row
        # streamto_inner_loop
        names = Tuple(name(x) for x in selectedcols)
        types = Tuple{(T(x) for x in selectedcols)...}
        inds = Tuple(:($(@val sourceindex(x))) for x in selectedcols)
        vals = :(vals = NamedTuple{$names, $types}(($(inds...),)))
        push!(streamto_inner_loop.args,
            :(Data.streamto!(sink, Data.Row, $vals, sinkrowoffset + sinkrow, 0, Val{$knownrows})))
    end

    if knownrows && (S == Data.Field || S == Data.Row) && !sorted(code)
        # println("generating loop w/ known rows...")
        return quote
            $pre_outer_loop
            sinkrow = 1
            for sourcerow = $starting_row:$rows
                $streamfrom_inner_loop
                $streamto_inner_loop
                @label end_of_loop
                sinkrow += 1
            end
        end
    else
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
end

gettransforms(sch, d::AbstractDict{<:Integer, <:Base.Callable}) = d

function gettransforms(sch, d::AbstractDict{<:AbstractString, <:Base.Callable})
    D = Base.typename(typeof(d)).wrapper
    D(sch[x] => f for (x, f) in d)
end

const TRUE = x->true

function Data.stream!(source::So, ::Type{Si}, args...;
                        append::Bool=false,
                        transforms::AbstractDict=Dict{Int, Function}(),
                        filter::Function=TRUE,
                        columns::Vector=[],
                        actions=[], limit=nothing, offset=nothing,
                        kwargs...) where {So, Si}
    if isempty(transforms)
        acts = actions
    elseif isempty(actions)
        # exclude transform columns, add scalarcomputed transform column w/ same name
        sch = Data.schema(source)
        trns = gettransforms(sch, transforms)
        acts = Vector{Any}(undef, sch.cols)
        names = Data.header(sch)
        for col in 1:sch.cols
            acts[col] = if haskey(trns, col)
                (name=names[col], compute=trns[col], computeargs=(col,))
            else
                (col=col,)
            end
        end
    else
        throw(ArgumentError("`transforms` is deprecated, use only `actions` to specify column transformations"))
    end
    sch = Data.schema(source)
    types = Data.anytypes(sch, weakrefstrings(Si))
    header = Data.header(sch)
    q = Query(types, header, acts, limit, offset)
    return Data.stream!(source, q, Si, args...; append=append, kwargs...)
end

function Data.stream!(source::So, sink::Si;
                        append::Bool=false,
                        transforms::AbstractDict=Dict{Int, Function}(),
                        filter::Function=TRUE,
                        actions=[], limit=nothing, offset=nothing,
                        columns::Vector=[]) where {So, Si}
    if isempty(transforms)
        acts = actions
    elseif isempty(actions)
        # exclude transform columns, add scalarcomputed transform column w/ same name
        sch = Data.schema(source)
        trns = gettransforms(sch, transforms)
        acts = Vector{Any}(undef, sch.cols)
        names = Data.header(sch)
        for col in 1:sch.cols
            acts[col] = if haskey(trns, col)
                (name=names[col], compute=trns[col], computeargs=(col,))
            else
                (col=col,)
            end
        end
    else
        throw(ArgumentError("`transforms` is deprecated, use only `actions` to specify column transformations"))
    end
    sch = Data.schema(source)
    types = Data.anytypes(sch, weakrefstrings(Si))
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
                sink = S(sinkschema, sinkstreamtype, append, args...; reference=Data.reference(q), kwargs...)
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
                sink = S(sink, sinkschema, sinkstreamtype, append; reference=Data.reference(q))
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
    # println(remove_line_number_nodes(r))
    return r
end

#TODO: figure out non-unrolled case
  # use Any[ ] to store row vals until stream out or push
#TODO: spread, gather, sample, analytic functions
    # gather: (name=:gathered, gather=true, args=(1,2,3))
    # spread: (spread=1, value=2)
