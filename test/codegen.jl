
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


# mutable struct RowIterator{names,T}
#     nt::NamedTuple{names, T}
# end
# Base.start(ri::RowIterator) = 1
# @generated function Base.next(ri::RowIterator{names, T}, row::Int) where {names, T}
#     S = Tuple{map(eltype, T.parameters)...}
#     r = :(convert($S, tuple($((:($(Symbol("v$i")) = getfield(ri.nt, $i)[row]; $(Symbol("v$i")) isa Null ? null : $(Symbol("v$i"))) for i = 1:nfields(T))...))))
#     return r
# end
# Base.done(ri::RowIterator, i::Int) = i > length(getfield(ri.nt, 1))
# @generated function Base.next(ri::RowIterator{names,T}, row::Int) where {names, T}
#     NT = NamedTuple{names}
#     S = Tuple{map(eltype, T.parameters)...}
#     r = :(Base.namedtuple($NT, convert($S, tuple($((:($(Symbol("v$i")) = getfield(ri.nt, $i)[row]; $(Symbol("v$i")) isa Null ? null : $(Symbol("v$i"))) for i = 1:nfields(T))...)))...))
#     return r
# end
#
# @generated function Base.next(ri::RowIterator{names,T,S}, i::Int)::S where {names, T, S}
#     # NT = NamedTuple{names}
#     args = Expr[:(v = getfield(nt, $i)[row]; ifelse(v isa Null, null, v)) for i = 1:nfields(T)]
#     return :((args...))
#     # R = NamedTuple{names, Tuple{map(eltype, T.parameters)...}}
#     # return :(convert($R, Base.namedtuple($NT, $(args...))))
# end
# check codegen of:
  # < 500 columns
  # homogenous datasets > 500 columns
  # > 500 columns

  # Data.Field vs. Data.Column

  # knownrows vs. not

  #