module DataStreams

importall Base.Operators

export PointerString
immutable PointerString <: AbstractString
    ptr::Ptr{UInt8}
    len::Int
end
export NULLSTRING
const NULLSTRING = PointerString(C_NULL,0)
Base.show(io::IO, x::PointerString) = print(io,x == NULLSTRING ? "PointerString(\"\")" : "PointerString(\"$(bytestring(x.ptr,x.len))\")")
Base.showcompact(io::IO, x::PointerString) = print(io,x == NULLSTRING ? "\"\"" : "\"$(bytestring(x.ptr,x.len))\"")
Base.endof(x::PointerString) = x.len
Base.string(x::PointerString) = x == NULLSTRING ? "" : "$(bytestring(x.ptr,x.len))"
Base.convert(::Type{ASCIIString}, x::PointerString) = convert(ASCIIString, string(x))
Base.convert(::Type{UTF8String}, x::PointerString) = convert(UTF8String, string(x))

using NullableArrays

export stream!
function stream!
end

export IOSource, IOSink

abstract IOSource <: IO
abstract IOSink   <: IO

typealias SourceOrSink Union{IOSource,IOSink}

schema(io::SourceOrSink) = io.schema
header(io::SourceOrSink) = header(schema(io))
types(io::SourceOrSink) = types(schema(io))
Base.size(io::IOSource) = size(schema(io))

export Schema
type Schema
    header::Vector{UTF8String}  # column names
    types::Vector{DataType}     # types of columns
    rows::Int                   # the number of rows in the dataset
    cols::Int                   # of columns in a dataset
    function Schema(header::Vector,types::Vector{DataType},rows::Integer=0,cols::Integer=0)
        length(header) == length(types) || throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
        cols = length(header)
        header = UTF8String[utf8(x) for x in header]
        return new(header,types,rows,cols)
    end
end

Schema(types::Vector{DataType},rows::Integer=0,cols::Integer=0) = Schema(UTF8String["Column$i" for i = 1:length(types)],types,rows,cols)
const EMPTYSCHEMA = Schema(UTF8String[],DataType[],0,0)
Schema() = EMPTYSCHEMA

header(sch::Schema) = sch.header
types(sch::Schema) = sch.types
Base.size(sch::Schema) = (sch.rows,sch.cols)
Base.size(sch::Schema,i::Int) = i == 1 ? sch.rows : i == 2 ? sch.cols : 0
==(s1::Schema,s2::Schema) = header(s1) == header(s2) && types(s1) == types(s2) && size(s1) == size(s2)

export DataTable
type DataTable <: IOSource
    schema::Schema
    index::Vector{Int}
    ints::Vector{NullableVector{Int}}
    floats::Vector{NullableVector{Float64}}
    ptrstrings::Vector{NullableVector{PointerString}}
    strings::Vector{NullableVector{UTF8String}}
    dates::Vector{NullableVector{Date}}
    datetimes::Vector{NullableVector{DateTime}}
    any::Vector{NullableVector{Any}}
    other::Any # sometimes you just need to keep a reference around...
end

# Constructors
function DataTable(schema::Schema,other=0)
    # allocate data
    rows, cols = size(schema)
    ints = NullableVector{Int}[]
    floats = NullableVector{Float64}[]
    ptrstrings = NullableVector{PointerString}[]
    strings = NullableVector{UTF8String}[]
    dates = NullableVector{Date}[]
    datetimes = NullableVector{DateTime}[]
    any = NullableVector{Any}[]
    index = Array(Int,cols)
    for col = 1:cols
        T = schema.types[col]
        if T == Int
            push!(ints,NullableArray(T, rows))
            index[col] = length(ints)
        elseif T == Float64
            push!(floats,NullableArray(T, rows))
            index[col] = length(floats)
        elseif T == PointerString
            push!(ptrstrings,NullableArray(T, rows))
            index[col] = length(ptrstrings)
        elseif T <: AbstractString
            push!(strings,NullableArray(UTF8String, rows))
            index[col] = length(strings)
        elseif T == Date
            push!(dates,NullableArray(T, rows))
            index[col] = length(dates)
        elseif T == DateTime
            push!(datetimes,NullableArray(T, rows))
            index[col] = length(datetimes)
        else
            push!(any,NullableArray(T, rows))
            index[col] = length(any)
        end
    end
    return DataTable(schema,index,ints,floats,ptrstrings,strings,dates,datetimes,any,other)
end

DataTable(types::Vector{DataType},rows::Int,other=0) = DataTable(Schema(types,rows),other)
DataTable(source::IOSource) = DataTable(schema(source))

# Interface

# column access
export column
function column(dt::DataTable, j, T)
    (0 < j < length(dt.index)+1) || throw(ArgumentError("column index $i out of range"))
    return unsafe_column(dt, j, T)
end
@inline unsafe_column(dt::DataTable, j, ::Type{Int64}) = (@inbounds col = dt.ints[dt.index[j]]; return col)
@inline unsafe_column(dt::DataTable, j, ::Type{Float64}) = (@inbounds col = dt.floats[dt.index[j]]; return col)
@inline unsafe_column(dt::DataTable, j, ::Type{PointerString}) = (@inbounds col = dt.ptrstrings[dt.index[j]]; return col)
@inline unsafe_column{T<:AbstractString}(dt::DataTable, j, ::Type{T}) = (@inbounds col = dt.strings[dt.index[j]]; return col)
@inline unsafe_column(dt::DataTable, j, ::Type{Date}) = (@inbounds col = dt.dates[dt.index[j]]; return col)
@inline unsafe_column(dt::DataTable, j, ::Type{DateTime}) = (@inbounds col = dt.datetimes[dt.index[j]]; return col)
@inline unsafe_column(dt::DataTable, j, T) = (@inbounds col = dt.any[dt.index[j]]; return col)

# cell indexing
function Base.getindex(dt::DataTable, i, j)
    col = column(dt, j, types(dt)[j])
    return col[i]
end

end # module

#TODO
 # define show for Schema, DataStream
