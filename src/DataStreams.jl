"""
The `DataStreams.jl` packages defines a data processing framework based on Sources, Sinks, and the `Data.stream!` function.

`DataStreams` defines the common infrastructure leveraged by individual packages to create systems of various
data sources and sinks that talk to each other in a unified, consistent way.

The workflow enabled by the `DataStreams` framework involves:
 * constructing new `Source` types to allow streaming data from files, databases, etc.
 * `Data.stream!` those datasets to newly created or existing `Sink` types
 * convert `Sink` types that have received data into new `Source` types
 * continue to `Data.stream!` from `Source`s to `Sink`s

The typical approach for a new package to "satisfy" the DataStreams interface is to:
 * Define a `Source` type that wraps an "true data source" (i.e. a file, database table/query, etc.) and fulfills the `Source` interface (see `?Data.Source`)
 * Define a `Sink` type that can create or write data to an "true data source" and fulfills the `Sink` interface (see `?Data.Sink`)
 * Define appropriate `Data.stream!(::Source, ::Sink)` methods as needed between various combinations of Sources and Sinks;
   i.e. define `Data.stream!(::NewPackage.Source, ::CSV.Sink)` and `Data.stream!(::CSV.Source, ::NewPackage.Sink)`
"""
module DataStreams

export Data, PointerString
module Data

export PointerString
"""
A custom "weakref" string type that only stores a Ptr{UInt8} and len::Int.
Allows for extremely efficient string parsing/movement in some cases between files and databases.
***Please note that no original reference is kept to the parent string/memory, so `PointerString`s become unsafe
once the parent object goes out of scope (i.e. loses a reference to it)***
"""
immutable PointerString{T} <: AbstractString
    ptr::Ptr{T}
    len::Int
end

const NULLSTRING = PointerString(Ptr{UInt8}(0),0)
const NULLSTRING16 = PointerString(Ptr{UInt16}(0),0)
const NULLSTRING32 = PointerString(Ptr{UInt32}(0),0)
Base.show{T}(io::IO, ::Type{PointerString{T}}) = print(io,"PointerString{$T}")
Base.show(io::IO, x::PointerString) = print(io,x == NULLSTRING ? "PointerString(\"\")" : "PointerString(\"$(bytestring(x.ptr,x.len))\")")
Base.show(io::IO, x::PointerString{UInt16}) = print(io,x == NULLSTRING16 ? "PointerString(\"\")" : "PointerString(\"$(utf16(x.ptr,x.len))\")")
Base.show(io::IO, x::PointerString{UInt32}) = print(io,x == NULLSTRING32 ? "PointerString(\"\")" : "PointerString(\"$(utf32(x.ptr,x.len))\")")
Base.showcompact(io::IO, x::PointerString) = print(io,x == NULLSTRING ? "\"\"" : "\"$(bytestring(x.ptr,x.len))\"")
Base.showcompact(io::IO, x::PointerString{UInt16}) = print(io,x == NULLSTRING16 ? "\"\"" : "\"$(utf16(x.ptr,x.len))\"")
Base.showcompact(io::IO, x::PointerString{UInt32}) = print(io,x == NULLSTRING32 ? "\"\"" : "\"$(utf32(x.ptr,x.len))\"")
Base.endof(x::PointerString) = x.len
Base.string(x::PointerString) = x == NULLSTRING ? "" : bytestring(x.ptr,x.len)
Base.string(x::PointerString{UInt16}) = x == NULLSTRING16 ? utf16("") : utf16(x.ptr,x.len)
Base.string(x::PointerString{UInt32}) = x == NULLSTRING32 ? utf32("") : utf32(x.ptr,x.len)
Base.convert(::Type{ASCIIString}, x::PointerString) = convert(ASCIIString, string(x))
Base.convert(::Type{UTF8String}, x::PointerString) = convert(UTF8String, string(x))
Base.convert(::Type{UTF16String}, x::PointerString) = convert(UTF16String, string(x))
Base.convert(::Type{UTF32String}, x::PointerString) = convert(UTF32String, string(x))
Base.convert(::Type{PointerString{UInt8}}, x::Union{ASCIIString,UTF8String}) = PointerString(pointer(x.data),sizeof(x))
Base.convert(::Type{PointerString{UInt16}}, x::UTF16String) = PointerString(pointer(x.data),sizeof(x))
Base.convert(::Type{PointerString{UInt32}}, x::UTF32String) = PointerString(pointer(x.data),sizeof(x))

"""
A `Data.Source` type holds data that can be read/queried/parsed/viewed/streamed; i.e. a "true data source"
To clarify, there are two distinct types of "source":
  1) the "true data source", which would be the file, database, API, structure, etc; i.e. the actual data
  2) the `Data.Source` julia object that wraps the "true source" and provides the `DataStreams` interface

`Source` types have two different types of constructors:
  1) "independent constructors" that wrap "true data sources"
  2) "sink constructors" where a `Data.Sink` object that has received data is turned into a new `Source` (useful for chaining data processing tasks)

`Source`s also have a, currently implicit, notion of state:
  * `BEGINNING`: a `Source` is in this state immediately after being constructed and is ready to be used; i.e. ready to read/parse/query/stream data from it
  * `READING`: the ingestion of data from this `Source` has started and has not finished yet
  * `DONE`: the ingestion process has exhausted all data expected from this `Source`

The `Data.Source` interface includes the following:
 * `Data.schema(::Data.Source) => Data.Schema`; typically the `Source` type will store the `Data.Schema` directly, but this isn't strictly required
 * `Data.reset!(::Data.Source)`; used to reset a `Source` type from `READING` or `DONE` to the `BEGINNING` state, ready to be read from again
 * `eof(::Data.Source)`; indicates whether the `Source` type is in the `DONE` state; i.e. all data has been exhausted from this source

"""
abstract Source

function reset!
end
function isdone
end
# TODO: flesh out the "fetching"/"reading"/"ingesting" interface for `Source`s
 # getting results: getrow(::Source) => Any[], getcol(::Source) => T[], getfield(::Source) => T; getrows, getcols
 # iterating through results: eachrow(::Source) => Any[], eachcol(::Source) => T[], eachfield(::Source) => T
 # printing/viewing results: printrow(::Source), printcol(::Source), printfield(::Source); printrows, printcols
# function getfield
# end
# function getrow
# end
# function eachfield
# end
# function eachrow
# end
# function printfield
# end
# function printrow
# end

"""
A `Data.Sink` type represents a data destination; i.e. an "true data source" such as a database, file, API endpoint, etc.

There are two broad types of `Sink`s:
  1) "new sinks": an independent `Sink` constructor creates a *new* "true data source" that can be streamed to
  2) "existing sinks": the `Sink` wraps an already existing "true data source" (or `Source` object that wraps an "true data source").
    Upon construction of these Sinks, there is no new creation of "true data source"s; the "ulitmate data source" is simply wrapped to replace or append to

`Sink`s also have notions of state:
  * `BEGINNING`: the `Sink` is freshly constructed and ready to stream data to; this includes initial metadata like column headers
  * `WRITING`: data has been streamed to the `Sink`, but is still open to receive more data
  * `DONE`: the `Sink` has been closed and can no longer receive data

The `Data.Sink` interface includes the following:
 * `Data.schema(::Data.Sink) => Data.Schema`; typically the `Sink` type will store the `Data.Schema` directly, but this isn't strictly required
"""
abstract Sink
typealias SourceOrSink Union{Source,Sink}

"""
`Data.stream!(::Data.Source, ::Data.Sink)` starts transfering data from a newly constructed `Source` type to a newly constructed `Sink` type.
Data transfer typically continues until `eof(source) == true`, i.e. the `Source` is exhausted, at which point the `Sink` is closed and may
no longer receive data. See individual `Data.stream!` methods for more details on specific `Source`/`Sink` combinations.
"""
function stream!#(::Source,::Sink)
end

# creates a new Data.Sink of type `T` according to `source` schema and streams data to it
function Data.stream!{T<:Data.Sink}(source::Data.Source, ::Type{T})
    sink = T(Data.schema(source))
    return Data.stream!(source,sink)
end

"""
A `Data.Schema` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)
Access to `Data.Schema` fields includes:
 * `Data.header(schema)` to return the header/column names in a `Data.Schema`
 * `Data.types(schema)` to return the column types in a `Data.Schema`
 * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`
"""
type Schema
    header::Vector{UTF8String}  # column names
    types::Vector{DataType}     # Julia types of columns
    rows::Int                   # number of rows in the dataset
    cols::Int                   # number of columns in a dataset
    metadata::Dict{Any,Any}     # for any other metadata we'd like to keep around
    function Schema(header::Vector,types::Vector{DataType},rows::Integer=0,metadata::Dict=Dict())
        length(header) == length(types) || throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
        cols = length(header)
        header = UTF8String[utf8(x) for x in header]
        return new(header,types,rows,cols,metadata)
    end
end

Schema(header,types::Vector{DataType},rows::Integer=0,meta::Dict=Dict()) = Schema(UTF8String[i for i in header],types,rows,meta)
Schema(types::Vector{DataType},rows::Integer=0,meta::Dict=Dict()) = Schema(UTF8String["Column$i" for i = 1:length(types)],types,rows,meta)
const EMPTYSCHEMA = Schema(UTF8String[],DataType[],0,Dict())
Schema() = EMPTYSCHEMA

header(sch::Schema) = sch.header
types(sch::Schema) = sch.types
Base.size(sch::Schema) = (sch.rows,sch.cols)
Base.size(sch::Schema,i::Int) = ifelse(i == 1, sch.rows, ifelse(i == 2, sch.cols, 0))
import Base.==
==(s1::Schema,s2::Schema) = header(s1) == header(s2) && types(s1) == types(s2) && size(s1) == size(s2)

const MAX_COLUMN_WIDTH = 100
function Base.show(io::IO, schema::Schema)
    println(io, "$(schema.rows)x$(schema.cols) Data.Schema:")
    if schema.cols <= 0
        println(io)
    else
        max_col_len = min(MAX_COLUMN_WIDTH,maximum([length(col) for col in header(schema)]))
        for (nam, typ) in zip(header(schema),types(schema))
            println(io, length(nam) > MAX_COLUMN_WIDTH ? string(nam[1:chr2ind(nam,MAX_COLUMN_WIDTH-3)],"...") : lpad(nam, max_col_len, ' '), ", ", typ)
        end
    end
end
const MAX_NUM_OF_COLS_TO_PRINT = 10
function Base.showcompact(io::IO, schema::Schema)
    nms = header(schema)
    typs = types(schema)
    cols = size(schema,2)
    println(io, "$(schema.rows)x$(schema.cols) Data.Schema:")
    max_col_lens = [min(div(MAX_COLUMN_WIDTH,2),length(nm)) for nm in nms]
    max_col_lens = [max(max_col_lens[i],length(string(typs[i])))+1 for i = 1:cols]
    upper = min(MAX_NUM_OF_COLS_TO_PRINT,cols)
    cant_print_all = MAX_NUM_OF_COLS_TO_PRINT < cols
    for i = 1:upper
        nm = nms[i]
        print(io, length(nm) > max_col_lens[i] ? string(nm[1:chr2ind(nm,max_col_lens[i]-3)],"...") : lpad(nm, max_col_lens[i], ' '), ifelse(i == upper, ifelse(cant_print_all," ...\n","\n"), ","))
    end
    for i = 1:upper
        print(io, lpad(string(typs[i]), max_col_lens[i], ' '), ifelse(i == upper, ifelse(cant_print_all," ...\n","\n"), ","))
    end
end

"Returns the `Data.Schema` for `io`"
schema(io::SourceOrSink) = io.schema # by default, we assume the `Source`/`Sink` stores the schema directly
"Returns the header/column names (if any) associated with a specific `Source` or `Sink`"
header(io::SourceOrSink) = header(schema(io))
"Returns the column types associated with a specific `Source` or `Sink`"
types(io::SourceOrSink) = types(schema(io))
"Returns the (# of rows,# of columns) associated with a specific `Source` or `Sink`"
Base.size(io::Source) = size(schema(io))
Base.size(io::Source,i) = size(schema(io),i)

"""
a generic `Source` type that fulfills the DataStreams interface
wraps any kind of Julia structure `T`; by default `T` = Vector{NullableVector}
"""
type Table{T} <: Source
    schema::Schema
    data::T
    other::Any # for other metadata, references, etc.
end

function Base.show{T}(io::IO, x::Table{T})
    eltyp = T == Vector{NullableVector} ? "" : "{$T}"
    println(io, "Data.Table$eltyp:")
    showcompact(io, x.schema)
    show(io, x.data)
end

using NullableArrays
Base.string(x::NullableVector{Data.PointerString}) = NullableArray(UTF8String[x for x in x.values], x.isnull)

# Constructors
function Table(schema::Schema,other=0)
    rows, cols = size(schema)
    return Table(schema,NullableVector[NullableArray(T, rows) for T in types(schema)],other)
end
# define our own default Data.stream! method since Data.Table <: Source
function Data.stream!(source::Data.Source, ::Type{Data.Table})
    sink = Data.Table(Data.schema(source))
    return Data.stream!(source,sink)
end

Table(header::Vector,types::Vector{DataType},rows::Integer=0,other=0) = Table(Schema(header,types,rows),other)
Table(types::Vector{DataType},rows::Integer=0,other=0) = Table(Schema(types,rows),other)
Table(source::Source) = Table(schema(source))
function Table{T}(A::AbstractArray{T,2},header=UTF8String[],other=0)
    rows, cols = size(A)
    types = DataType[typeof(A[1,col]) for col = 1:cols]
    data = NullableVector[convert(NullableArray{types[col],1},NullableArray(A[:,col])) for col = 1:cols]
    header = isempty(header) ? UTF8String["Column$i" for i = 1:cols] : header
    return Table(Schema(header,types,rows),data,other)
end

# Interface
# column access
function column(dt::Table, j, T)
    (0 < j < dt.schema.cols+1) || throw(ArgumentError("column index $i out of range"))
    return unsafe_column(dt, j, T)
end
@inline unsafe_column{T}(dt::Table, j, ::Type{T}) = (@inbounds col = dt.data[j]::NullableVector{T}; return col)

function Base.getindex(dt::Table, i, j)
    col = column(dt, j, types(dt)[j])
    return col[i]
end

end # module Data
end # module DataStreams

# Define conversions between Data.Table and DataFrame is the latter is defined
if isdefined(:DataFrames)
    DataFrames.DataFrame(dt::DataStreams.Data.Table) = DataFrame(convert(Vector{Any},DataArray[DataArray(x.values,x.isnull) for x in dt.data]),Symbol[symbol(x) for x in DataStreams.Data.header(dt)])
    function DataStreams.Data.Table(df::DataFrames.DataFrame)
        rows, cols = size(df)
        schema = DataStreams.Data.Schema(UTF8String[string(c) for c in names(df)],DataType[eltype(i) for i in df.columns],rows)
        data = NullableArrays.NullableVector[NullableArrays.NullableArray(x.data,convert(Vector{Bool},x.na)) for x in df.columns]
        return DataStreams.Data.Table(schema,data,0)
    end
end
