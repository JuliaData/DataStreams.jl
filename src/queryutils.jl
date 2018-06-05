function remove_line_number_nodes(ex)
    todelete = Int[]
    for (i, arg) in enumerate(ex.args)
        if typeof(arg) <: LineNumberNode
            push!(todelete, i)
        elseif typeof(arg) <: Expr && arg.head != :macrocall
            remove_line_number_nodes(arg)
        end
    end
    deleteat!(ex.args, todelete)
    return ex
end

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
const AAA = 0x80 # unused

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
            aos = Vector{$K}(undef, len)
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
    left::Union{Node{T}, Nothing}
    right::Union{Node{T}, Nothing}
    node::Union{Node, Nothing}
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
