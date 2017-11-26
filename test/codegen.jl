
reload("DataStreams"); reload("Feather")
source = Feather.Source("test.feather")
sink = Si = NamedTuple
transforms = Dict{Int,Function}()
append = false
args = kwargs = ()
source_schema = DataStreams.Data.schema(source)
sink_schema, transforms2 = DataStreams.Data.transform(source_schema, transforms, true);
sinkstreamtype = DataStreams.Data.Column
sink = Si(sink_schema, sinkstreamtype, append, args...; kwargs...);
columns = []
filter = x->true
@code_warntype DataStreams.Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2, filter, columns)


source = ODBC.Source(dsn, "show databases")
sink = Si = NamedTuple
transforms = Dict{Int,Function}()
append = false
args = kwargs = ()
source_schema = DataStreams.Data.schema(source)
sink_schema, transforms2 = DataStreams.Data.transform(source_schema, transforms, true);
sinkstreamtype = DataStreams.Data.Column
sink = Si(sink_schema, sinkstreamtype, append, args...; kwargs...);
columns = []
filter = x->true
@code_warntype DataStreams.Data.stream!(source, sinkstreamtype, sink, source_schema, sink_schema, transforms2, filter, columns)

# DataStreams tests
sink = Si = Sink
transforms = Dict{Int,Function}()
append = false
args = kwargs = ()
source_schema = DataStreams.Data.schema(source)
sink_schema, transforms2 = DataStreams.Data.transform(source_schema, transforms, true);
sinkstreamtype = DataStreams.Data.Field
sink = Si(sink_schema, sinkstreamtype, append, args...; kwargs...);
columns = []
filter = x->true
@code_warntype DataStreams.Data.stream!(source, sinkstreamtype, sink, source_schema, 0, transforms2, filter, columns)


# CSV
reload("CSV")
T = Int64
@time source = CSV.Source("/Users/jacobquinn/Downloads/randoms_$(T).csv";)
sink = Si = NamedTuple
transforms = Dict{Int,Function}()
append = false
args = kwargs = ()
source_schema = DataStreams.Data.schema(source)
sink_schema, transforms2 = DataStreams.Data.transform(source_schema, transforms, true);
sinkstreamtype = DataStreams.Data.Field
sink = Si(sink_schema, sinkstreamtype, append, args...; kwargs...);
columns = []
filter = x->true
@code_warntype DataStreams.Data.stream!(source, sinkstreamtype, sink, source_schema, 0, transforms2, filter, columns)

mutable struct RowIterator{names,T}
    nt::NamedTuple{names, T}
end
Base.start(ri::RowIterator) = 1
@generated function Base.next(ri::RowIterator{names, T}, row::Int) where {names, T}
    S = Tuple{map(eltype, T.parameters)...}
    r = :(convert($S, tuple($((:($(Symbol("v$i")) = getfield(ri.nt, $i)[row]; $(Symbol("v$i")) isa Missing ? missing : $(Symbol("v$i"))) for i = 1:nfields(T))...))))
    return r
end
Base.done(ri::RowIterator, i::Int) = i > length(getfield(ri.nt, 1))
@generated function Base.next(ri::RowIterator{names,T}, row::Int) where {names, T}
    NT = NamedTuple{names}
    S = Tuple{map(eltype, T.parameters)...}
    r = :(Base.namedtuple($NT, convert($S, tuple($((:($(Symbol("v$i")) = getfield(ri.nt, $i)[row]; $(Symbol("v$i")) isa Missing ? missing : $(Symbol("v$i"))) for i = 1:nfields(T))...)))...))
    return r
end

@generated function Base.next(ri::RowIterator{names,T,S}, i::Int)::S where {names, T, S}
    # NT = NamedTuple{names}
    args = Expr[:(v = getfield(nt, $i)[row]; ifelse(v isa Missing, missing, v)) for i = 1:nfields(T)]
    return :((args...))
    # R = NamedTuple{names, Tuple{map(eltype, T.parameters)...}}
    # return :(convert($R, Base.namedtuple($NT, $(args...))))
end
# check codegen of:
  # < 500 columns
  # homogenous datasets > 500 columns
  # > 500 columns

  # Data.Field vs. Data.Column

  # knownrows vs. not

  #

  # test codegen based on # of columns



function f(v::Vector{Int8}, n)
  s = Int8(0)
  @inbounds for i = 1:n
    s += v[i]
  end
  return s
end
function f(v::Vector{Union{Void, Int8}}, n)
  s = Int8(0)
  @inbounds for i = 1:n
    t = Base.arrayref(v, i)
    s += t isa Void ? Int8(0) : t
  end
  return s
end
using BenchmarkTools
n = 100
@benchmark f($(Vector{Int8}(n)), $n)
V = Vector{Union{Void, Int8}}(n)
@benchmark f($(V), $n)
@code_llvm f(Vector{Int8}(10), 10)
@code_llvm f(Vector{Union{Void, Int8}}(10), 10)
function f(v)
  return Base.arrayref(v, 1)
end
