using DataStreams
using Base.Test
using Compat
using NullableArrays

ROWS = 2
COLS = 3

sch = Data.Schema([Int, Int, Int], ROWS)
data = [ 1 2 ; 10 20 ]

@test Data.header(sch) == Compat.UTF8String["Column1", "Column2", "Column3"]
@test Data.types(sch) == [Int, Int, Int]
@test size(sch) == (ROWS, COLS)

@test sch == Data.Schema([Int, Int, Int], ROWS)

print(sch)

src_tb = Data.Table(sch)

for i = 1:ROWS, j = 1:COLS
    src_tb.data[j][i] = i * j
end

print(src_tb)

@test get(getindex(src_tb, 2, 3)) == 6

"""
A generic `Sink` type that fulfills the DataStreams interface
wraps any kind of Julia structure `T`; by default `T` = Vector{NullableVector}
"""
type TableSink{T} <: Data.Sink
    schema::Data.Schema
    data::T
    other::Any # for other metadata, references, etc.
end

function TableSink(sch::Data.Schema)
    rows, cols = size(sch)
    TableSink(sch, NullableVector[NullableArray(T, rows) for T in Data.types(sch)], 0)
end

TableSink(s::Data.Source) = TableSink(schema(s))

import DataStreams.Data.stream!

function Data.stream!(src::Data.Table, snk::TableSink)
    # TODO: this could be improved considering different source Schema.
    snk.data = src.data
end

snk_tb = Data.stream!(src_tb, Type(TableSink))
