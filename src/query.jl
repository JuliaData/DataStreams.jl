# function rle(t::Tuple)
#     typs = Pair{Type, Int}[]
#     len = 1
#     T = t[1]
#     for i = 2:length(t)
#         TT = t[i]
#         if T == TT
#             len += 1
#         else
#             push!(typs, T=>len)
#             T = TT
#             len = 1
#         end
#     end
#     push!(typs, T=>len)
#     return typs
# end
tuplesubset(tup, ::Tuple{}) = ()
tuplesubset(tup, inds) = (tup[inds[1]], tuplesubset(tup, Base.tail(inds))...)

import Base.|
|(::Type{A}, ::Type{B}) where {A, B} = Union{A, B}
have(x) = x !== nothing

const QueryCodeType = UInt8
const UNUSED         = 0x00
const SELECTED       = 0x01
const SCALARFILTERED = 0x02
const AGGFILTERED    = 0x04
const SCALARCOMPUTED = 0x08
const AGGCOMPUTED    = 0x10
const SORTED         = 0x20
const GROUPED        = 0x40
const AAA = 0x80

unused(code::QueryCodeType) = code === UNUSED

concat!(A, val) = push!(A, val)
concat!(A, a::AbstractArray) = append!(A, a)

filter(func, val) = func(val)
function filter(filtered, func, val)
    @inbounds for i = 1:length(val)
        !filtered[i] && continue
        filtered[i] = func(val[i])
    end
end

calculate(func, vals...) = func(vals...)
calculate(func, vals::AbstractArray...) = func.(vals...)

# Data.Field/Data.Row aggregating
@generated function aggregate(aggregates, aggkeys, aggvalues)
    # if @generated
        default = Tuple(:($T[]) for T in aggvalues.parameters)
        q = quote
            entry = get!(aggregates, aggkeys, tuple($(default...)))
            $((:(push!(entry[$i], aggvalues[$i]);) for i = 1:length(aggvalues.parameters))...)
        end
        # println(q)
        return q
    # else
    #     entry = get!(aggregates, aggkeys, Tuple(typeof(val)[] for val in aggvalues))
    #     for (A, val) in zip(entry, aggvalues)
    #         push!(A, val)
    #     end
    # end
end
# Data.Column aggregating
@generated function aggregate(aggregates::Dict{K}, aggkeys::T, aggvalues) where {K, T <: NTuple{N, Vector{TT} where TT}} where {N}
    # if @generated
        len = length(aggkeys.parameters)
        vallen = length(aggvalues.parameters)
        inds = Tuple(:(aggkeys[$i][i]) for i = 1:len)
        valueinds = Tuple(:(aggvalues[$i][sortinds]) for i = 1:vallen)
        default = Tuple(:($T[]) for T in aggvalues.parameters)
        q = quote
            # SoA => AoS
            len = length(aggkeys[1])
            aos = Vector{$K}(len)
            for i = 1:len
                aos[i] = tuple($(inds...))
            end
            sortinds = sortperm(aos)
            aos = aos[sortinds]
            sortedvalues = tuple($(valueinds...))
            key = aos[1]
            n = 1
            for i = 2:len
                key2 = aos[i]
                if isequal(key, key2)
                    continue
                else
                    entry = get!(aggregates, key, tuple($(default...)))
                    $((:(append!(entry[$i], sortedvalues[$i][n:i-1]);) for i = 1:vallen)...)
                    n = i
                    key = key2
                end
            end
            entry = get!(aggregates, key, tuple($(default...)))
            $((:(append!(entry[$i], sortedvalues[$i][n:end]);) for i = 1:vallen)...)
        end
        # println(q)
        return q
    # else
    #     println("slow path")
    # end
end

# Nested binary search tree for multi-column sorting
mutable struct Node{T}
    inds::Vector{Int}
    value::T
    left::Union{Node{T}, Void}
    right::Union{Node{T}, Void}
    node::Union{Node, Void}
end
Node(rowind, ::Tuple{}) = nothing
Node(rowind, t::Tuple) = Node([rowind], t[1], nothing, nothing, Node(rowind, Base.tail(t)))

insert!(node, rowind, dir, ::Tuple{}) = nothing
function insert!(node, rowind, dir, tup)
    key = tup[1]
    if key == node.value
        push!(node.inds, rowind)
        insert!(node.node, rowind, Base.tail(dir), Base.tail(tup))
    elseif (key < node.value) == dir[1]
        if have(node.left)
            insert!(node.left, rowind, dir, tup)
        else
            node.left = Node(rowind, tup)
        end
    else
        if have(node.right)
            insert!(node.right, rowind, dir, tup)
        else
            node.right = Node(rowind, tup)
        end
    end
    return
end
function inds(n::Node, ind, A::Vector{Int})
    if have(n.left)
        inds(n.left, ind, A)
    end
    if have(n.node)
        inds(n.node, ind, A)
    else
        for i in n.inds
            A[ind[]] = i
            ind[] += 1
        end
    end
    if have(n.right)
        inds(n.right, ind, A)
    end
end

function sort(sortinds, sortkeys::Tuple)
    dirs = Tuple(p.second for p in sortkeys)
    root = Node(1, Tuple(p.first[1] for p in sortkeys))
    for i = 2:length(sortkeys[1].first)
        @inbounds insert!(root, i, dirs, Tuple(p.first[i] for p in sortkeys))
    end
    inds(root, Ref(1), sortinds)
    return sortinds
end

struct Sort{ind, asc} end
sortind(::Type{Sort{ind, asc}}) where {ind, asc} = ind
sortasc(::Type{Sort{ind, asc}}) where {ind, asc} = asc

"""
Represents a column used in a Data.Query for querying a Data.Source

Passed as the `actions` argument as an array of NamedTuples to `Data.query(source, actions, sink)`

Options include:

  * `col::Integer`: reference to a source column index
  * `name`: the name the column should have in the resulting query, if none is provided, it will be auto-generated
  * `T`: the type of the column, if not provided, it will be inferred from the source
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
    filter::(Function|Void)
    having::(Function|Void)
    compute::(Function|Void)
    aggregate::(Function|Void)
end

function QueryColumn(sourceindex, types=(), header=[];
                name=Symbol(""),
                T::Type=Any,
                sinkindex::(Integer|Void)=sourceindex,
                hide::Bool=false,
                filter::(Function|Void)=nothing,
                having::(Function|Void)=nothing,
                compute::(Function|Void)=nothing,
                computeaggregate::(Function|Void)=nothing,
                computeargs=nothing,
                sort::Bool=false,
                sortindex::(Integer|Void)=nothing,
                sortasc::Bool=true,
                group::Bool=false,
                aggregate::(Function|Void)=nothing,
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
        if have(arg)
            code |= c
        end
    end
    if have(compute) || have(computeaggregate)
        args = computeargs
        compute = have(compute) ? compute : computeaggregate
        T = Core.Inference.return_type(compute, have(compute) ? tuplesubset(types, args) : Tuple(Vector{T} for T in tuplesubset(types, args)))
        name = name == Symbol("") ? Symbol("Column$sinkindex") : Symbol(name)
    elseif have(aggregate)
        T = Any
        name = name == Symbol("") && length(header) >= sourceindex ? Symbol(header[sourceindex]) : Symbol(name)
    else
        T = (T == Any && length(types) >= sourceindex) ? types[sourceindex] : T
        name = name == Symbol("") && length(header) >= sourceindex ? Symbol(header[sourceindex]) : Symbol(name)
    end
    S = sort ? Sort{sortindex, sortasc} : nothing
    return QueryColumn{code, T, sourceindex, sinkindex, name, S, args}(filter, having, compute, aggregate)
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
end

# E type parameter is for a tuple of integers corresponding to
# column index inputs for aggcomputed columns
struct Query{code, S, T, E, L, O}
    source::S
    columns::T # Tuple{QueryColumn...}, columns are in *output* order (i.e. monotonically increasing by sinkindex)
end

function Query(source::S, actions, limit=nothing, offset=nothing) where {S}
    sch = Data.schema(source)
    types = Data.types(sch)
    header = Data.header(sch)
    len = length(types)
    outlen = length(types)
    columns = []
    cols = Set()
    extras = Set()
    aggcompute_extras = Set()
    si = 0
    outcol = 1
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

    return Query{querycode, S, typeof(columns), Tuple(aggcompute_extras), limit, offset}(source, columns)
end

"""
    Data.query(source, actions, sink=Data.Table, args...; append::Bool=false, limit=nothing, offset=nothing)

Query a valid DataStreams `Data.Source` according to query `actions` and stream the result into `sink`.
`limit` restricts the total number of rows streamed out, while `offset` will skip initial N rows.
`append=true` will cause the `sink` to _accumulate_ the additional query resultset rows instead of replacing any existing rows in the sink.

`actions` is an array of NamedTuples, w/ each NamedTuple including one or more of the following query arguments:

  * `col::Integer`: reference to a source column index
  * `name`: the name the column should have in the resulting query, if none is provided, it will be auto-generated
  * `T`: the type of the column, if not provided, it will be inferred from the source
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
function query(source, actions, sink=Table, args...; append::Bool=false, limit::(Integer|Void)=nothing, offset::(Integer|Void)=nothing)
    q = Query(source, actions, limit, offset)
    sink = Data.stream!(q, sink, args...; append=append)
    return Data.close!(sink)
end

unwk(T, wk) = T
unwk(::Type{WeakRefString{T}}, wk) where {T} = wk ? WeakRefString{T} : String

"Compute the Data.Schema of the resultset of executing Data.Query `q` against its source"
function schema(q::Query{code, S, columns, e, limit, offset}, wk=true) where {code, S, columns, e, limit, offset}
    types = Tuple(unwk(T(col), wk) for col in columns.parameters if selected(col))
    header = Tuple(String(name(col)) for col in columns.parameters if selected(col))
    off = have(offset) ? offset : 0
    rows = size(Data.schema(q.source), 1)
    rows = have(limit) ? min(limit, rows - off) : rows - off
    rows = (scalarfiltered(code) | grouped(code)) ? missing : rows
    return Schema(types, header, rows)
end

# function subset(T, I, i)
#     if Base.tuple_type_head(I) == i
#         head = Base.tuple_type_head(T)
#         tail = Base.tuple_type_tail(I)
#         return tail == Tuple{} ? Tuple{head} : Base.tuple_type_cons(head, subset(Base.tuple_type_tail(T), tail, i + 1))
#     else
#         return subset(T, I, i + 1)
#     end
# end

# function tupletypesubset(::Type{T}, ::Type{I}) where {T, I}
#     if @generated
#         TT = subset(T, I, 1)
#         return :($TT)
#     else
#         Tuple{(T.parameters[i] for i in I.parameters)...}
#     end
# end

codeblock() = Expr(:block)
macro vals(ex)
    return esc(:(Symbol(string("vals", $ex))))
end
macro val(ex)
    return esc(:(Symbol(string("val", $ex))))
end

# generate the entire streaming loop, according to any QueryColumns passed by the user
function generate_loop(knownrows, S, code, columns, extras, sourcetypes, limit, offset)
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
    cols = collect(columns.parameters)
    sourceinds = sortperm(cols, by=x->sourceindex(x))
    sourcecolumns = [ind=>cols[ind] for ind in sourceinds]

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
        SF = S == Data.Row ? Data.Field : S
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
    # now we loop through query result columns, to build up code blocks for streaming to Data.Sink
    for (ind, col) in enumerate(columns.parameters)
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
            push!(pre_aggregation_loop.args, :($(@vals out) = Vector{$(T(col))}(length(aggregates))))
            push!(aggregation_inner_loop.args, :($(@vals out)[i] = k[$(length(aggregationkeys))]))
        elseif !aggcomputed(col) && (selected(col) || sourceindex(col) in extras)
            push!(aggregationvalues, col)
            push!(pre_aggregation_loop.args, :($(@vals out) = Vector{Any}(length(aggregates))))
            if selected(col)
                push!(aggregation_inner_loop.args, :($(@vals out)[i] = q.columns[$ind].aggregate(v[$(length(aggregationvalues))])))
            end
        elseif aggcomputed(col)
            push!(aggregationcomputed, ind=>col)
            push!(pre_aggregation_loop.args, :($(@vals out) = Vector{$(T(col))}(length(aggregates))))
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
    # streamfrom_inner_loop
    if S == Data.Column
        push!(streamfrom_inner_loop.args, :(cur_row = length($(@val sourceindex(firstcol)))))
    end
    # aggregation_loop
    if grouped(code)
        for (ind, col) in aggregationcomputed
            valueargs = Tuple(:(v[$(findfirst(x->sourceindex(x) == i, aggregationvalues))]) for i in args(col))
            push!(aggregation_inner_loop.args, :($(@vals sinkindex(col))[i] = q.columns[$ind].compute($(valueargs...))))
        end
        if aggfiltered(code)
            unshift!(post_aggregation_loop.args, :(filtered = fill(true, length(aggregates))))
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
                vals = @static if isdefined(Core, :NamedTuple)
                        :(vals = NamedTuple{$names, $types}(($(inds...),)))
                    else
                        exprs = [:($nm::$typ) for (nm, typ) in zip(names, types.parameters)]
                        nt = NamedTuples.make_tuple(exprs)
                        :(vals = $nt($(inds...)))
                    end
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
        vals = @static if isdefined(Core, :NamedTuple)
                :(vals = NamedTuple{$names, $types}(($(inds...),)))
            else
                exprs = [:($nm::$typ) for (nm, typ) in zip(names, types.parameters)]
                nt = NamedTuples.make_tuple(exprs)
                :(vals = $nt($(inds...)))
            end
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

function Data.stream!(q::Query{code, So}, ::Type{Si}, args...; append::Bool=false, kwargs...) where {code, So, Si}
    S = datatype(Si)
    sinkstreamtypes = Data.streamtypes(S)
    for sinkstreamtype in sinkstreamtypes
        if Data.streamtype(datatype(So), sinkstreamtype)
            wk = weakrefstrings(S)
            sourceschema = Data.schema(q.source)
            sinkschema = Data.schema(q, wk)
            if wk
                sink = S(sinkschema, sinkstreamtype, append, args...; reference=Data.reference(q), kwargs...)
            else
                sink = S(sinkschema, sinkstreamtype, append, args...; kwargs...)
            end
            sourcerows = size(sourceschema, 1)
            sinkrows = size(sinkschema, 1)
            sinkrowoffset = ifelse(append, ifelse(ismissing(sourcerows), sinkrows, max(0, sinkrows - sourcerows)), 0)
            return Data.stream!(q, sinkstreamtype, sink, sourceschema, sinkrowoffset)
        end
    end
    throw(ArgumentError("`source` doesn't support the supported streaming types of `sink`: $sinkstreamtypes"))
end

@generated function Data.stream!(q::Query{code, So, columns, extras, limit, offset}, ::Type{S}, sink,
                        source_schema::Data.Schema{R, T1}, sinkrowoffset) where {S <: Data.StreamType, R, T1, code, So, columns, extras, limit, offset}
    types = T1.parameters
    sourcetypes = Tuple(types)
    # runlen = rle(sourcetypes)
    T = isempty(types) ? Any : types[1]
    homogeneous = all(i -> (T === i), types)
    N = length(types)
    knownrows = R && !scalarfiltered(code) && !grouped(code)
    RR = R ? Int : Missing
    r = quote
        rows, cols = size(source_schema)::Tuple{$RR, Int}
        Data.isdone(q.source, 1, 1, rows, cols) && return sink
        source = q.source
        sourcetypes = $sourcetypes
        N = $N
        try
            $(generate_loop(knownrows, S, code, columns, extras, sourcetypes, limit, offset))
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
    # @show columns
    # println(r)
    return r
end

#TODO: figure out non-unrolled case
  # use Any[ ] to store row vals until stream out or push
#TODO: spread, gather, sample, analytic functions
    # gather: (name=:gathered, gather=true, args=(1,2,3))
    # spread: (spread=1, value=2)