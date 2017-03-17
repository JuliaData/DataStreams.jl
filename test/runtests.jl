using Base.Test, DataStreams, DataTables, NullableArrays

if !isdefined(Core, :String)
    typealias String UTF8String
end

ROWS = 2
COLS = 3

sch = Data.Schema([Int, Int, Int], ROWS)
data = [ 1 2 ; 10 20 ]

@test Data.header(sch) == String["Column1", "Column2", "Column3"]
@test Data.types(sch) == [Int, Int, Int]
@test size(sch) == (ROWS, COLS)

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

function Data.stream!(src::DataTable, snk::TableSink)
    # TODO: this could be improved considering different source Schema.
    snk.data = src.columns
end

src_tb = DataTable()
snk = TableSink(sch)

snk_tb = Data.stream!(src_tb, snk)

schema = Data.Schema(["A", "B"], [Int64, String])
transforms = Dict{String, Function}("A" => (x) -> -x, "B" => lowercase)
sink_schema, transforms2 = Data.transform(schema, transforms)
@test transforms2 == [transforms["A"], transforms["B"]]

schema = Data.Schema(["A", "B"], [Int64, String])
transforms = Dict{String, Function}("B" => lowercase)
sink_schema, transforms2 = Data.transform(schema, transforms)
@test transforms2 == [identity, transforms["B"]]
