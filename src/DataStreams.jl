module DataStreams

using NullableArrays

export stream!
stream!() = nothing # so we have something to export

export IOSource, IOSink
abstract IOSource <: IO
abstract IOSink   <: IO

export Schema
type Schema
    header::Vector{UTF8String}  # column names
    types::Vector{DataType}     # types of columns
    rows::Int                   # the number of rows in the dataset
    cols::Int                   # of columns in a dataset
    function Schema(header::Vector,types::Vector{DataType},rows::Int=0,cols::Int=0)
        length(header) == length(types) || throw(ArgumentError("length(header): $(length(header)) must == length(types): $(length(types))"))
        cols = length(header)
        header = UTF8String[utf8(x) for x in header]
        return new(header,types,rows,cols)
    end
end

Schema(types::Vector{DataType},rows::Int=0,cols::Int=0) = Schema(UTF8String["Column$i" for i = 1:length(types)],types,rows,cols)

header(sch::Schema) = sch.header
types(sch::Schema) = sch.types
Base.size(sch::Schema) = (sch.rows,sch.cols)
Base.size(sch::Schema,i::Int) = i == 1 ? sch.rows : i == 2 ? sch.cols : 0

# TODO: define Base.show for Schema
export DataStream
type DataStream{T} <: IO
    schema::Schema
    data::T
end

# Constructors
function DataStream{T}(data::T)
    # build Schema
    M, N = size(data)
    header = UTF8String["Column$i" for i = 1:N]
    types = DataType[typeof(data[1,col]) for col = 1:N]
    schema = Schema(header,types,M,N)
    return DataStream(schema,data)
end

function DataStream(schema::Schema)
    # allocate data
    M, N = size(schema)
    return DataStream(schema,NullableVector[NullableArray(schema.types[i],M) for i = 1:N])
end

DataStream(types::Vector{DataType},rows::Int) = DataStream(Schema(types,rows))

# Interface
@inline Base.getindex(ds::DataStream, i, j) = (@inbounds v = ds.data[j][i]; return v)
@inline Base.setindex!(ds::DataStream, v, i, j) = (@inbounds ds.data[j][i] = v; return v)
# Base.getindex{T}(ds::DataStream{Array{NullableArrays.NullableArray{T,1},1}}, i::Int, j::Int) = ds.data[j][i]

end # module
