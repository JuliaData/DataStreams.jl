function tuplesubset(tup, inds)
    if @generated
        N = length(inds.parameters)
        args = Any[:(tup[inds[$i]]) for i = 1:N]
        return Expr(:new, :(NTuple{$N, DataType}), args...)
    else
        Tuple(tup[inds[i]] for i in inds)
    end
end

import Base.|
|(::Type{A}, ::Type{B}) where {A, B} = Union{A, B}
have(x) = x !== nothing

const QueryCodeType = UInt8
const UNUSED = 0x00
unused(code::QueryCodeType) = code === UNUSED
const SELECTED = 0x01
const SCALARFILTERED = 0x02
const AGGFILTERED = 0x04
const SCALARCALCULATED = 0x08
const AGGCALCULATED = 0x10
const SORTED = 0x20
const AGGREGATED = 0x40
const AAA = 0x80

for (f, c) in (:selected=>SELECTED,
               :scalarfiltered=>SCALARFILTERED,
               :aggfiltered=>AGGFILTERED,
               :scalarcalculated=>SCALARCALCULATED,
               :aggcalculated=>AGGCALCULATED,
               :sorted=>SORTED,
               :aggregated=>AGGREGATED)
    @eval $f(code::QueryCodeType) = (code & $c) > 0
    @eval $f(x) = $f(code(x))
end

filter(func, val) = func(val)
function filter(filtered, func, val)
    @inbounds for i = 1:length(val)
        !filtered[i] && continue
        filtered[i] = func(val[i])
    end
end

calculate(func, vals...) = func(vals...)
calculate(func, vals::AbstractArray...) = func.(vals...)

function aggregate(aggregates, aggkeys, aggvalues)
    if @generated
        default = Tuple(T[] for T in aggvalues.parameters)
        quote
            entry = get!(aggregates, aggkeys, $default)
            $((:(push!(entry[$i], aggvalues[$i]);) for i = 1:length(aggvalues.parameters))...)
        end
    else
        entry = get!(aggregates, aggkeys, Tuple(typeof(val)[] for val in aggvalues))
        for (A, val) in zip(entry, aggvalues)
            push!(A, val)
        end
    end
end
@generated function aggregate(aggregates::Dict{K}, aggkeys::T, aggvalues) where {K, T <: NTuple{N, Vector{TT} where TT}} where {N}
    # if @generated
        len = length(aggkeys.parameters)
        vallen = length(aggvalues.parameters)
        inds = Tuple(:(aggkeys[$i][i]) for i = 1:len)
        valueinds = Tuple(:(aggvalues[$i][sortinds]) for i = 1:vallen)
        default = Tuple(:(T[]) for T in aggvalues.parameters)
        q = quote
            # SoA => AoS
            len = length(aggkeys[1])
            aos = Vector{$K}(len)
            for i = 1:len
                aos[i] = Tuple($(inds...))
            end
            sortinds = sortperm(aos)
            aos = aos[sortinds]
            sortedvalues = tuple($(valueinds...))
            key = aos[1]
            n = 1
            for i = 2:len
                key2 = aos[i]
                if key == key2
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
    # println("inserting $key on row=$rowind, sorted=$(dir[1] ? "asc" : "desc")")
    # @show key, node.value, dir[1]
    if key == node.value
        push!(node.inds, rowind)
        # println("moving down...")
        insert!(node.node, rowind, Base.tail(dir), Base.tail(tup))
    elseif (key < node.value) == dir[1]
        if have(node.left)
            # println("moving left...")
            insert!(node.left, rowind, dir, tup)
        else
            # println("setting left node")
            node.left = Node(rowind, tup)
        end
    else
        if have(node.right)
            # println("moving right...")
            insert!(node.right, rowind, dir, tup)
        else
            # println("setting right node")
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
    @show root
    @show sortinds
    return sortinds
end

struct Sort{ind, asc} end
sortind(::Type{Sort{ind, asc}}) where {ind, asc} = ind
sortasc(::Type{Sort{ind, asc}}) where {ind, asc} = asc

struct QueryColumn{code, T, index, outputindex, name, sort, args}
    filter::(Function|Void)
    having::(Function|Void)
    compute::(Function|Void)
    aggregate::(Function|Void)
end

function QueryColumn(index, types=(), header=[];
                name=Symbol(""),
                T::Type=Any,
                outputindex::(Integer|Void)=index,
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
    for (arg, c) in (!hide=>SELECTED, sort=>SORTED, group=>AGGREGATED)
        arg && (code |= c)
    end
    for (arg, c) in ((filter, SCALARFILTERED),
                     (having, AGGFILTERED),
                     (compute, SCALARCALCULATED),
                     (computeaggregate, AGGCALCULATED))
        if have(arg)
            code |= c
        end
    end
    if have(compute) || have(computeaggregate)
        args = computeargs
        compute = have(compute) ? compute : computeaggregate
        T = Core.Inference.return_type(compute, have(compute) ? tuplesubset(types, args) : Tuple(Vector{T} for T in tuplesubset(types, args)))
        name = name == Symbol("") ? Symbol("Column$outputindex") : Symbol(name)
    else
        T = (T == Any && length(types) >= index) ? types[index] : T
        name = name == Symbol("") && length(header) >= index ? Symbol(header[index]) : Symbol(name)
    end
    S = sort ? Sort{sortindex, sortasc} : nothing
    return QueryColumn{code, T, index, outputindex, name, S, args}(filter, having, compute, aggregate)
end

code(::Type{<:QueryColumn{c}}) where {c} = c
codeT(::Type{<:QueryColumn{c, t}}) where {c, t} = (c, t)
T(::Type{<:QueryColumn{c, t}}) where {c, t} = t
index(::Type{<:QueryColumn{c, t, i}}) where {c, t, i} = i
outputindex(::Type{<:QueryColumn{c, t, i, o}}) where {c, t, i, o} = o
name(::Type{<:QueryColumn{c, t, i, o, n}}) where {c, t, i, o, n} = n
sort(::Type{<:QueryColumn{c, t, i, o, n, s}}) where {c, t, i, o, n, s} = s
args(::Type{<:QueryColumn{c, t, i, o, n, s, a}}) where {c, t, i, o, n, s, a} = a

struct Query{code, S, T}
    source::S
    columns::T # Tuple{QueryColumn...}, columns are in *output* order (i.e. monotonically increasing by outputindex)
end

# action:
 # (col=1, name="renamed_column")
 # (col=2, )
 # (col=0, compute=x->x*2)
function Query(source::S, actions...) where {S}
    sch = Data.schema(source)
    types = Data.types(sch)
    header = Data.header(sch)
    len = length(types)
    columns = []
    si = 0
    for (i, x) in enumerate(actions)
        sortindex = get(x, :sortindex) do
            sorted = get(x, :sort, false)
            if sorted
                si += 1
                return si
            else
                return nothing
            end
        end
        push!(columns, QueryColumn(
                        get(()->(len += 1; return len), x, :col), 
                        types, header; 
                        outputindex=i,
                        sortindex=sortindex,
                        ((k, getfield(x, k)) for k in keys(x))...)
        )
    end
    columns = Tuple(columns)
    querycode = UNUSED
    for col in columns
        c = code(typeof(col))
        querycode |= c
    end
    return Query{querycode, S, typeof(columns)}(source, columns)
end

function query(source, actions...)
    q = Query(source, actions...)
    Data.stream!(q, NamedTuple)
end

unwk(T, wk) = T
unwk(::Type{WeakRefString{T}}, wk) where {T} = wk ? WeakRefString{T} : String

function schema(q::Query{code, S, columns}, wk=true) where {code, S, columns}
    # TODO: make this less hacky
    types = Tuple(unwk(T.parameters[2], wk) for T in columns.parameters if selected(T))
    header = Tuple(String(T.parameters[5]) for T in columns.parameters if selected(T))
    return Schema(types, header, (scalarfiltered(code) | aggregated(code)) ? missing : size(Data.schema(q.source), 1))
end

# Data.Source interface implementation
Data.reference(q::Query) = Data.reference(q.source)
function Data.isdone(q::Query, row, col, rows, cols)
    rows, cols = size(Data.schema(q.source))
    Data.isdone(q.source, row, col, rows, cols)
end
Data.isdone(q::Query, row, col) = Data.isdone(q.source, row, col)
Data.streamtype(::Type{<:Query{code, S}}, ::Type{Data.Column}) where {code, S} = Data.streamtype(S)
Data.streamtype(::Type{<:Query{code, S}}, ::Type{Data.Field}) where {code, S} = Data.streamtype(S)

Data.streamfrom(q::Query, ::Type{Data.Column}, ::Type{T}, row, col) where {T} = Data.streamfrom(q.source, Data.Column, T, row, col)
Data.streamfrom(q::Query, ::Type{Data.Field}, ::Type{T}, row, col) where {T} = Dadta.streamfrom(q.source, Data.Field, T, row, col)
Data.reset!(q::Query) = Data.reset!(q.source)
Data.accesspattern(::Type{<:Query{code, S}}) where {code, S} = Data.accesspatern(S)

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

function unroll_streamfrom_loop(loop, ::Type{S}, column::Type{<:QueryColumn{code, T, index, outputindex, name, sort, args}}, tail, ind, firstfilter) where {S, code, T, index, outputindex, name, sort, args}
    while ind < index && index != 0
        push!(loop.args, :(Data.skipfield!(source, $S, sourcetypes[$ind], row, $ind)))
        ind += 1
    end
    if scalarcalculated(code)
        computeargs = Tuple(Symbol("val_$c") for c in args)
        push!(loop.args, :($(Symbol("val_$index")) = calculate(source.columns[$outputindex].compute, $(computeargs...))))
    elseif !aggcalculated(code)
        push!(loop.args, :($(Symbol("val_$index")) = Data.streamfrom(source, $S, $T, row, $index)))
    end
    if scalarfiltered(code)
        if S == Data.Field
            push!(loop.args, quote
                ff = filter(source.columns[$outputindex].filter, $(Symbol("val_$index")))
                if !ff
                    Data.skiprow!(source, $S, $T, row, $index + 1)
                    @goto end_of_loop
                    continue
                end
            end)
        else
            if firstfilter
                push!(loop.args, :(filtered = fill(true, length($(Symbol("val_$index"))))))
                firstfilter = false
            end
            push!(loop.args, :(filter(filtered, source.columns[$outputindex].filter, $(Symbol("val_$index")))))
        end
    end
    tail === Tuple{} && return
    unroll_streamfrom_loop(loop, S, Base.tuple_type_head(tail), Base.tuple_type_tail(tail), ind + 1, firstfilter)
end

function unroll_streamto_loop(loop, ::Type{S}, ::Type{<:QueryColumn{code, T, index}}, tail, outcol, knownrows, filtered) where {S, code, T, index}
    if selected(code)
        if (S == Data.Column) & outcol == 1
            push!(loop.args, :(cur_row = length($(Symbol("val_$index")))))
        end
        if filtered
            push!(loop.args, :(Data.streamto!(sink, $S, $(Symbol("val_$index"))[filtered], sinkrowoffset + row, $outcol, Val{$knownrows})))
        else
            push!(loop.args, :(Data.streamto!(sink, $S, $(Symbol("val_$index")), sinkrowoffset + row, $outcol, Val{$knownrows})))
        end
        outcol += 1
    end
    tail === Tuple{} && return
    unroll_streamto_loop(loop, S, Base.tuple_type_head(tail), Base.tuple_type_tail(tail), outcol, knownrows, filtered)
end

push(vals, val) = push!(vals, val)
push(vals, val::AbstractArray) = append!(vals, val)

function unroll_push_loop(loop, ::Type{S}, ::Type{<:QueryColumn{code, T, index, outputindex}}, tail, outcol, filtered) where {S, code, T, index, outputindex}
    if selected(code)
        if (S == Data.Column) & outcol == 1
            push!(loop.args, :(cur_row = length($(Symbol("val_$index")))))
        end
        if filtered
            push!(loop.args, :(push($(Symbol("vals_$outputindex")), $(Symbol("val_$index"))[filtered])))
        else
            push!(loop.args, :(push($(Symbol("vals_$outputindex")), $(Symbol("val_$index")))))
        end
        outcol += 1
    end
    tail === Tuple{} && return
    unroll_push_loop(loop, S, Base.tuple_type_head(tail), Base.tuple_type_tail(tail), outcol, filtered)
end

function generate_inner_loop(::Type{S}, sourcecolumns, columns, cols, code, knownrows) where {S}
    loop = quote end
    if cols < 100
        # streamfrom, calc, and filter
        unroll_streamfrom_loop(loop, S, Base.tuple_type_head(sourcecolumns), Base.tuple_type_tail(sourcecolumns), 1, true)
        if aggregated(code)
            if (S == Data.Column) & scalarfiltered(code)
                aggkeys =   Tuple(:($(Symbol("val_$(index(col))"))[filtered]) for col in sourcecolumns.parameters if aggregated(col))
                aggvalues = Tuple(:($(Symbol("val_$(index(col))"))[filtered]) for col in sourcecolumns.parameters if !aggregated(col) & !aggcalculated(col))
            else
                aggkeys =   Tuple(Symbol("val_$(index(col))") for col in sourcecolumns.parameters if aggregated(col))
                aggvalues = Tuple(Symbol("val_$(index(col))") for col in sourcecolumns.parameters if !aggregated(col) & !aggcalculated(col))
            end
            # collect aggregate key value(s) and add entry(s) to aggregates dict
            push!(loop.args, :(aggregate(aggregates, ($(aggkeys...),), ($(aggvalues...),))))
            # push!(loop.args, :(@show aggregates))
            for x in sourcecolumns.parameters
                if aggregated(x)
                    push!(loop.args, :(cur_row = length($(Symbol("val_$(index(x))")))))
                    break
                end
            end
        elseif sorted(code)
            # store row(s) temporarily
            unroll_push_loop(loop, S, Base.tuple_type_head(columns), Base.tuple_type_tail(columns), 1, scalarfiltered(code))
        else
            unroll_streamto_loop(loop, S, Base.tuple_type_head(columns), Base.tuple_type_tail(columns), 1, knownrows, scalarfiltered(code))
        end
    else
        # TODO non-unrolled
        error("> 100 columns not supported yet")
    end
    return loop
end

function generate_loop(knownrows, ::Type{S}, code, columns, sourcecolumns, inner_loop) where {S <: Data.StreamType}
    if knownrows && S == Data.Field
        # println("generating loop w/ known rows...")
        loop = quote
            for row = 1:rows
                $inner_loop
                @label end_of_loop
            end
        end
    else
        loop = quote end
        # println("generating loop w/ unknown rows...")
        aggvaluemap = Dict()
        if aggregated(code)
            # allocate aggregates Dict
            aggkeytype = Tuple{(T(x) for x in sourcecolumns.parameters if aggregated(x))...}
            aggvaluetype = Tuple{(Vector{T(x)} for x in sourcecolumns.parameters if !aggregated(x) & !aggcalculated(x))...}
            aggvalues = collect(x for x in sourcecolumns.parameters if !aggregated(x) & !aggcalculated(x))
            aggvaluemap = Dict(index(x)=>i for (i, x) in enumerate(aggvalues))
            push!(loop.args, :(aggregates = Dict{$aggkeytype, $aggvaluetype}()))
        elseif sorted(code)
            # allocate temp column buffers
            for col in columns.parameters
                if selected(col)
                    TT = T(col)
                    push!(loop.args, :($(Symbol("vals_$(outputindex(col))")) = $TT[]))
                end
            end
        end
        push!(loop.args, quote
            row = cur_row = 1
            while true
                $inner_loop
                @label end_of_loop
                row += cur_row # will be 1 for Data.Field, length(val) for Data.Column
                Data.isdone(source, row, cols, rows, cols) && break
            end
            Data.setrows!(source, row)
        end)
        # if aggregated, aggregate
        if aggregated(code)
            groupedinds = Dict(outputindex(x)=>T(x) for x in columns.parameters if aggregated(x))
            # @show groupedinds
            aggregatedinds = Dict(outputindex(x)=>i for (i, x) in enumerate(Base.filter(x->selected(x) & !aggregated(x) & !aggcalculated(x), columns.parameters)))
            # @show aggregatedinds
            computedinds = Dict(outputindex(x)=>(T(x), Tuple(:(v[$(aggvaluemap[i])]) for i in args(x))) for x in columns.parameters if aggcalculated(x))
            # @show computedinds
            aggfilteredinds = [outputindex(x) for x in columns.parameters if aggfiltered(x)]
            if aggfiltered(code)
                filtblk = quote
                    filtered = fill(true, length(aggregates))
                    $((:(filter(filtered, source.columns[$i].having, $(Symbol("vals_$i")))) for i in aggfilteredinds)...)
                end
            else
                filtblk = :(nothing)
            end
            # finalcolumntypes = 
            push!(loop.args, quote
                # allocate final grouped column vectors
                $((:($(Symbol("vals_$i")) = Vector{$j}(length(aggregates));) for (i, j) in groupedinds)...)
                # allocate final aggregated column vectors; need to infer type from aggregate function
                # TODO: we could call Inference in the QueryColumn constructor and store the computed T in the type
                $((:($(Symbol("vals_$i")) = Vector{Any}(length(aggregates));) for (i, j) in aggregatedinds)...)
                # allocate final computed aggregated column vectors; already inferred types
                $((:($(Symbol("vals_$i")) = Vector{$(j[1])}(length(aggregates));) for (i, j) in computedinds)...)
                # aggregated = Dict(k=>Tuple($((:(source.columns[$i].aggregate(v[$j])) for (i, j) in nongroupedinds)...)) for (k, v) in aggregates)
                for (i, (k, v)) in enumerate(aggregates)
                    # set grouped column values
                    $((:($(Symbol("vals_$i"))[i] = k[$i]) for (i, j) in groupedinds)...)
                    # aggregate and set aggregated column values
                    $((:($(Symbol("vals_$i"))[i] = source.columns[$i].aggregate(v[$j])) for (i, j) in aggregatedinds)...)
                    # compute & set computed aggregated columns
                    $((:($(Symbol("vals_$i"))[i] = source.columns[$i].compute($(j[2]...))) for (i, j) in computedinds)...)
                end
                # apply aggregate filters using filtered Bool vector; non-grouped + selected
                $filtblk
            end)
        end
        if sorted(code)
            if aggfiltered(code)
                sortkeys = Tuple(:($(Symbol("vals_$(outputindex(x))"))[filtered]=>$(sortasc(sort(x)))) for x in Base.sort(Base.filter(x->sorted(x), collect(columns.parameters)), by=x->sortind(sort(x))))
                push!(loop.args, quote
                    sortinds = fill(0, sum(filtered))
                    sort(sortinds, ($(sortkeys...),))
                end)
            else
                sortkeys = Tuple(:($(Symbol("vals_$(outputindex(x))"))=>$(sortasc(sort(x)))) for x in Base.sort(Base.filter(x->sorted(x), collect(columns.parameters)), by=x->sortind(sort(x))))
                firstind = 0
                for x in columns.parameters
                    if sorted(x)
                        firstind = outputindex(x)
                        break
                    end
                end
                push!(loop.args, quote
                    sortinds = fill(0, length($(Symbol("vals_$firstind"))))
                    sort(sortinds, ($(sortkeys...),))
                end) 
            end
        end
        if sorted(code) | aggregated(code)
            outcol = 1
            for col in columns.parameters
                if selected(col)
                    if sorted(code) & aggfiltered(code)
                        push!(loop.args, :(Data.streamto!(sink, $S, $(Symbol("vals_$(outputindex(col))"))[filtered][sortinds], sinkrowoffset + row, $outcol, Val{$knownrows})))
                    elseif sorted(code)
                        push!(loop.args, :(Data.streamto!(sink, $S, $(Symbol("vals_$(outputindex(col))"))[sortinds], sinkrowoffset + row, $outcol, Val{$knownrows})))
                    elseif aggfiltered(code)
                        push!(loop.args, :(Data.streamto!(sink, $S, $(Symbol("vals_$(outputindex(col))"))[filtered], sinkrowoffset + row, $outcol, Val{$knownrows})))
                    else
                        push!(loop.args, :(Data.streamto!(sink, $S, $(Symbol("vals_$(outputindex(col))")), sinkrowoffset + row, $outcol, Val{$knownrows})))
                    end
                    outcol += 1
                end
            end
        end
    end
    # println(macroexpand(loop))
    return loop
end

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

@generated function Data.stream!(source::Query{code, So, columns}, ::Type{S}, sink,
                        source_schema::Data.Schema{R, T1}, sinkrowoffset) where {S <: Data.StreamType, R, T1, code, So, columns}
    types = T1.parameters
    sourcetypes = Tuple(types)
    cols = collect(columns.parameters)
    colinds = map(index, cols)
    needed = [typeof(QueryColumn(i; T=sourcetypes[i], hide=true)) for i in Base.sort(unique(collect(Iterators.flatten(map(x->args(x), cols))))) if !(i in colinds)]
    if length(needed) > 0
        append!(needed, cols)
    else
        needed = cols
    end
    sourcecolumns = Tuple{Base.sort(needed, by=x->index(x) == 0 ? Inf : Float64(index(x)))...}
    # runlen = rle(sourcetypes)
    T = isempty(types) ? Any : types[1]
    homogeneous = all(i -> (T === i), types)
    N = length(types)
    knownrows = R
    RR = R ? Int : Missing
    r = quote
        rows, cols = size(source_schema)::Tuple{$RR, Int}
        Data.isdone(source, 1, 1, rows, cols) && return sink
        sourcetypes = $sourcetypes
        N = $N
        try
            $(generate_loop(knownrows, S, code, columns, sourcecolumns, generate_inner_loop(S, sourcecolumns, columns, N, code, knownrows)))
        catch e
            Data.cleanup!(sink)
            rethrow(e)
        end
        return sink
    end
    @show sourcecolumns
    # @show columns
    println(r)
    return r
end

#TODO: test Data.Field; write a Vector{NamedTuple} DataStreams implementation
#TODO: test w/ CSV (will need to adjust the `needed` calc for non-RandomAccess sources)
#TODO: figure out non-unrolled case
  # use Any[ ] to store row vals until stream out or push
#TODO: limit, offset, spread, gather, sample, analytic functions
#TODO: hook up frontend!