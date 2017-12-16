var documenterSearchIndex = {"docs": [

{
    "location": "index.html#",
    "page": "Home",
    "title": "Home",
    "category": "page",
    "text": ""
},

{
    "location": "index.html#DataStreams.jl-1",
    "page": "Home",
    "title": "DataStreams.jl",
    "category": "section",
    "text": "The DataStreams.jl package aims to define a generic and performant framework for the transfer of \"table-like\" data. (i.e. data that can, at least in some sense, be described by rows and columns).The framework achieves this by defining interfaces (i.e. a group of methods) for Data.Source types and methods to describe how they \"provide\" data; as well as Data.Sink types and methods around how they \"receive\" data. This allows Data.Sources and Data.Sinks to implement their interfaces separately, without needing to be aware of each other. The end result is an ecosystem of packages that \"automatically\" talk with each other, with adding an additional package not requiring changes to existing packages.Packages can have a single julia type implement both the Data.Source and Data.Sink interfaces, or two separate types can implement them separately. "
},

{
    "location": "index.html#DataStreams.Data.schema",
    "page": "Home",
    "title": "DataStreams.Data.schema",
    "category": "Function",
    "text": "Data.schema(s::Source) => Data.Schema\n\nReturn the Data.Schema of a source, which describes the # of rows & columns, as well as the column types of a dataset. Some sources like CSV.Source or SQLite.Source store the Data.Schema directly in the type, whereas others like DataFrame compute the schema on the fly.\n\nThe Data.Schema of a source is used in various ways during the streaming process:\n\nThe # of rows and if they are known are used to generate the inner streaming loop\nThe # of columns determine if the innermost streaming loop can be unrolled automatically or not\nThe types of the columns are used in loop unrolling to generate efficient and type-stable streaming\n\nSee ?Data.Schema for more details on how to work with the type.\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.isdone",
    "page": "Home",
    "title": "DataStreams.Data.isdone",
    "category": "Function",
    "text": "Data.isdone(source, row, col) => Bool\n\nChecks whether a source can stream additional fields/columns for a desired row and col intersection. Used during the streaming process, especially for sources that have an unknown # of rows, to detect when a source has been exhausted of data.\n\nData.Source types must at least implement:\n\nData.isdone(source::S, row::Int, col::Int)\n\nIf more convenient/performant, they can also implement:\n\nData.isdone(source::S, row::Int, col::Int, rows::Union{Int, Missing}, cols::Int)\n\nwhere rows and cols are the size of the source's schema when streaming.\n\nA simple example of how a DataFrame implements this is:\n\nData.isdone(df::DataFrame, row, col, rows, cols) = row > rows || col > cols\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.streamtype",
    "page": "Home",
    "title": "DataStreams.Data.streamtype",
    "category": "Function",
    "text": "Data.streamtype{T<:Data.Source, S<:Data.StreamType}(::Type{T}, ::Type{S}) => Bool\n\nIndicates whether the source T supports streaming of type S. To be overloaded by individual sources according to supported Data.StreamTypes. This is used in the streaming process to determine the compatability of streaming from a specific source to a specific sink. It also helps in determining the preferred streaming method, when matched up with the results of Data.streamtypes(s::Sink).\n\nFor example, if MyPkg.Source supported Data.Field streaming, I would define:\n\nData.streamtype(::Type{MyPkg.Source}, ::Type{Data.Field}) = true\n\nand/or for Data.Column streaming:\n\nData.streamtype(::Type{MyPkg.Source}, ::Type{Data.Column}) = true\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.reset!",
    "page": "Home",
    "title": "DataStreams.Data.reset!",
    "category": "Function",
    "text": "Data.reset!(source)\n\nResets a source into a state that allows streaming its data again. For example, for CSV.Source, the internal buffer is \"seek\"ed back to the start position of the csv data (after the column name headers). For SQLite.Source, the source SQL query is re-executed.\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.streamfrom",
    "page": "Home",
    "title": "DataStreams.Data.streamfrom",
    "category": "Function",
    "text": "Data.Source types must implement one of the following:\n\nData.streamfrom(source, ::Type{Data.Field}, ::Type{T}, row::Int, col::Int) where {T}\n\nData.streamfrom(source, ::Type{Data.Column}, ::Type{T}, col::Int) where {T}\n\nPerforms the actually streaming of data \"out\" of a data source. For Data.Field streaming, the single field value of type T at the intersection of row and col is returned. For Data.Column streaming, the column # col with element type T is returned.\n\nFor Data.Column, a source can also implement:\n\nData.streamfrom(source, ::Type{Data.Field}, ::Type{T}, row::Int, col::Int) where {T}\n\nwhere row indicates the # of rows that have already been streamed from the source.\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.accesspattern",
    "page": "Home",
    "title": "DataStreams.Data.accesspattern",
    "category": "Function",
    "text": "Data.accesspattern(source) => Data.RandomAccess | Data.Sequential\n\nreturns the data access pattern for a Data.Source.\n\nRandomAccess indicates that a source supports streaming data (via calls to Data.streamfrom) with arbitrary row/column values in any particular order.\n\nSequential indicates that the source only supports streaming data sequentially, starting w/ row 1, then accessing each column from 1:N, then row 2, and each column from 1:N again, etc.\n\nFor example, a DataFrame holds all data in-memory, and thus supports easy random access in any order. A CSV.Source however, is streaming the contents of a file, where rows must be read sequentially, and each column sequentially within each rows.\n\nBy default, sources are assumed to have a Sequential access pattern.\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.reference",
    "page": "Home",
    "title": "DataStreams.Data.reference",
    "category": "Function",
    "text": "Data.Source types can optionally implement\n\nData.reference(x::Source) => Vector{UInt8}\n\nwhere the type retruns a Vector{UInt8} that represents a memory block that should be kept in reference for WeakRefStringArrays.\n\nIn many streaming situations, the minimizing of data copying/movement is ideal. Some sources can provide in-memory access to their data in the form of a Vector{UInt8}, i.e. a single byte vector, that sinks can \"point to\" when streaming, instead of needing to always copy all the data. In particular, the WeakRefStrings package provides utilities for creating \"string types\" that don't actually hold their own data, but instead just \"point\" to data that lives elsewhere. As Strings can be some of the most expensive data structures to copy and move around, this provides excellent performance gains in some cases when the sink is able to leverage this alternative structure.\n\n\n\n"
},

{
    "location": "index.html#Data.Source-Interface-1",
    "page": "Home",
    "title": "Data.Source Interface",
    "category": "section",
    "text": "The Data.Source interface includes the following definitions:Data.schema\nData.isdone\nData.streamtype\nData.reset!\nData.streamfrom\nData.accesspattern\nData.reference"
},

{
    "location": "index.html#DataStreams.Data.Sink",
    "page": "Home",
    "title": "DataStreams.Data.Sink",
    "category": "Type",
    "text": "Represents a type that can have data streamed to it from Data.Sources.\n\nTo satisfy the Data.Sink interface, it must provide two constructors with the following signatures:\n\n[Sink](sch::Data.Schema, S::Type{StreamType}, append::Bool, args...; reference::Vector{UInt8}=UInt8[], kwargs...) => [Sink]\n[Sink](sink, sch::Data.Schema, S::Type{StreamType}, append::Bool; reference::Vector{UInt8}=UInt8[]) => [Sink]\n\nLet's break these down, piece by piece:\n\n[Sink]: this is your sink type, i.e. CSV.Sink, DataFrame, etc. You're defining a constructor for your sink type.\nsch::Data.Schema: in the streaming process, the schema of a Data.Source is provided to the sink in order to allow the sink to \"initialize\" properly in order to receive data according to the format in sch. This might mean pre-allocating space according to the # of rows/columns in the source, managing the sink's own schema to match sch, etc.\nS::Type{StreamType}: S represents the type of streaming that will occur from the Data.Source, either Data.Field or Data.Column\nappend::Bool: a boolean flag indicating whether the data should be appended to a sink's existing data store, or, if false, if the sink's data should be fully replaced by the incoming Data.Source's data\nargs...: In the 1st constructor form, args... represents a catchall for any additional arguments your sink may need to construct. For example, SQLite.jl defines Sink(sch, S, append, db, table_name), meaning that the db and table_name are additional required arguments in order to properly create an SQLite.Sink.\nreference::Vector{UInt8}: if your sink defined Data.weakrefstrings(sink::MySink) = true, then it also needs to be able to accept the reference keyword argument, where a source's memory block will be passed, to be held onto appropriately by the sink when streaming WeakRefStrings. If a sink does not support streaming WeakRefStrings (the default), the sink constructor doesn't need to support any keyword arguments.\nkwargs...: Similar to args..., kwargs... is a catchall for any additional keyword arguments you'd like to expose for your sink constructor, typically matching supported keyword arguments that are provided through the normal sink constructor\nsink: in the 2nd form, an already-constructed sink is passed in as the 1st argument. This allows efficient sink re-use. This constructor needs to ensure the existing sink is modified (enlarged, shrunk, schema changes, etc) to be ready to accept the incoming source data as described by sch.\n\nNow let's look at an example implementation from CSV.jl:\n\nfunction CSV.Sink(fullpath::AbstractString; append::Bool=false, headers::Bool=true, colnames::Vector{String}=String[], kwargs...)\n    io = IOBuffer()\n    options = CSV.Options(kwargs...)\n    !append && header && !isempty(colnames) && writeheaders(io, colnames, options)\n    return CSV.Sink(options, io, fullpath, position(io), !append && header && !isempty(colnames), colnames, length(colnames), append)\nend\n\nfunction CSV.Sink(sch::Data.Schema, T, append, file::AbstractString; reference::Vector{UInt8}=UInt8[], kwargs...)\n    sink = CSV.Sink(file; append=append, colnames=Data.header(sch), kwargs...)\n    return sink\nend\n\nfunction CSV.Sink(sink, sch::Data.Schema, T, append; reference::Vector{UInt8}=UInt8[])\n    sink.append = append\n    sink.cols = size(sch, 2)\n    !sink.header && !append && writeheaders(sink.io, Data.header(sch), sink.options, sink.quotefields)\n    return sink\nend\n\nIn this case, CSV.jl defined an initial constructor that just takes the filename with a few keyword arguments. The two required Data.Sink constructors are then defined. The first constructs a new Sink, requiring a file::AbstractString argument. We also see that CSV.Sink supports WeakRefString streaming by accepting a reference keyword argument (which is trivially implemented for CSV, since all data is simply written out to disk as text).\n\nFor the 2nd (last) constructor in the definitions above, we see the case where an existing sink is passed to CSV.Sink. The sink updates a few of its fields (sink.append = append), and some logic is computed to determine if the column headers should be written.\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.streamtypes",
    "page": "Home",
    "title": "DataStreams.Data.streamtypes",
    "category": "Function",
    "text": "Data.streamtypes(::Type{[Sink]}) => Vector{StreamType}\n\nReturns a vector of Data.StreamTypes that the sink is able to receive; the order of elements indicates the sink's streaming preference\n\nFor example, if my sink only supports Data.Field streaming, I would simply define:\n\nData.streamtypes(::Type{MyPkg.Sink}) = [Data.Field]\n\nIf, on the other hand, my sink also supported Data.Column streaming, and Data.Column streaming happend to be more efficient, I could define:\n\nData.streamtypes(::Type{MyPkg.Sink}) = [Data.Column, Data.Field] # put Data.Column first to indicate preference\n\nA third option is a sink that operates on entire rows at a time, in which case I could define:\n\nData.streamtypes(::Type{MyPkg.Sink}) = [Data.Row]\n\nThe subsequent Data.streamto! method would then require the signature Data.streamto!(sink::MyPkg.Sink, ::Type{Data.Row}, vals::NamedTuple, row, col, knownrows\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.streamto!",
    "page": "Home",
    "title": "DataStreams.Data.streamto!",
    "category": "Function",
    "text": "Data.streamto!(sink, S::Type{StreamType}, val, row, col)\n\nData.streamto!(sink, S::Type{StreamType}, val, row, col, knownrows)\n\nStreams data to a sink. S is the type of streaming (Data.Field, Data.Row, or Data.Column). val is the value or values (single field, row as a NamedTuple, or column, respectively) to be streamed to the sink. row and col indicate where the data should be streamed/stored.\n\nA sink may optionally define the method that also accepts the knownrows argument, which will be Val{true} or Val{false}, indicating whether the source streaming has a known # of rows or not. This can be useful for sinks that may know how to pre-allocate space in the cases where the source can tell the # of rows, or in the case of unknown # of rows, may need to stream the data in differently.\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.cleanup!",
    "page": "Home",
    "title": "DataStreams.Data.cleanup!",
    "category": "Function",
    "text": "Data.cleanup!(sink)\n\nSometimes errors occur during the streaming of data from source to sink. Some sinks may be left in an undesired state if an error were to occur mid-streaming. Data.cleanup! allows a sink to \"clean up\" any necessary resources in the case of a streaming error. SQLite.jl, for example, defines:\n\nfunction Data.cleanup!(sink::SQLite.Sink)\n    rollback(sink.db, sink.transaction)\n    return\nend\n\nSince a database transaction is initiated at the start of streaming, it must be rolled back in the case of streaming error.\n\nThe default definition is: Data.cleanup!(sink) = nothing\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.close!",
    "page": "Home",
    "title": "DataStreams.Data.close!",
    "category": "Function",
    "text": "Data.close!(sink) => sink\n\nA function to \"close\" a sink to streaming. Some sinks require a definitive time where data can be \"committed\", Data.close! allows a sink to perform any necessary resource management or commits to ensure all data that has been streamed is stored appropriately. For example, the SQLite package defines:\n\nfunction Data.close!(sink::SQLite.Sink)\n    commit(sink.db, sink.transaction)\n    return sink\nend\n\nWhich commits a database transaction that was started when the sink was initially \"opened\".\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.weakrefstrings",
    "page": "Home",
    "title": "DataStreams.Data.weakrefstrings",
    "category": "Function",
    "text": "Data.weakrefstrings(sink) => Bool\n\nIf a sink is able to appropriately handle WeakRefString objects, it can define:\n\nData.weakrefstrings(::Type{[Sink]}) = true\n\nto indicate that a source may stream those kinds of values to it. By default, sinks do not support WeakRefString streaming. Supporting WeakRefStrings corresponds to accepting the reference keyword argument in the required sink constructor method, see ?Data.Sink.\n\n\n\n"
},

{
    "location": "index.html#Data.Sink-Interface-1",
    "page": "Home",
    "title": "Data.Sink Interface",
    "category": "section",
    "text": "Data.Sink\nData.streamtypes\nData.streamto!\nData.cleanup!\nData.close!\nData.weakrefstrings"
},

{
    "location": "index.html#DataStreams.Data.stream!",
    "page": "Home",
    "title": "DataStreams.Data.stream!",
    "category": "Function",
    "text": "Data.stream!(source, sink; append::Bool=false, transforms=Dict())\n\nData.stream!(source, ::Type{Sink}, args...; append::Bool=false, transforms=Dict(), kwargs...)\n\nStream data from source to sink. The 1st definition assumes already constructed source & sink and takes two optional keyword arguments:\n\nappend::Bool=false: whether the data from source should be appended to sink\ntransforms::Dict: A dict with mappings between column # or name (Int or String) to a \"transform\" function. For Data.Field streaming, the transform function should be of the form f(x::T) => y::S, i.e. takes a single input of type T and returns a single value of type S. For Data.Column streaming, it should be of the form f(x::AbstractVector{T}) => y::AbstractVector{S}, i.e. take an AbstractVector with eltype T and return another AbstractVector with eltype S.\n\nFor the 2nd definition, the Sink type itself is passed as the 2nd argument (::Type{Sink}) and is constructed \"on-the-fly\", being passed args... and kwargs... like Sink(args...; kwargs...).\n\nWhile users are free to call Data.stream! themselves, oftentimes, packages want to provide even higher-level convenience functions.\n\nAn example of of these higher-level convenience functions are from CSV.jl:\n\nfunction CSV.read(fullpath::Union{AbstractString,IO}, sink::Type=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)\n    source = CSV.Source(fullpath; kwargs...)\n    sink = Data.stream!(source, sink, args...; append=append, transforms=transforms, kwargs...)\n    return Data.close!(sink)\nend\n\nfunction CSV.read(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...) where T\n    source = CSV.Source(fullpath; kwargs...)\n    sink = Data.stream!(source, sink; append=append, transforms=transforms)\n    return Data.close!(sink)\nend\n\nIn this example, CSV.jl defines it's own high-level function for reading from a CSV.Source. In these examples, a CSV.Source is constructed using the fullpath argument, along w/ any extra kwargs.... The sink can be provided as a type with args... and kwargs... that will be passed to its DataStreams constructor, like Sink(sch, streamtype, append, args...; kwargs...); otherwise, an already-constructed Sink can be provided directly (2nd example).\n\nOnce the source is constructed, the data is streamed via the call to Data.stream(source, sink; append=append, transforms=transforms), with the sink being returned.\n\nAnd finally, to \"finish\" the streaming process, Data.close!(sink) is closed, which returns the finalized sink. Note that Data.stream!(source, sink) could be called multiple times with different sources and the same sink, most likely with append=true being passed, to enable the accumulation of several sources into a single sink. A single Data.close!(sink) method should be called to officially close or commit the final sink.\n\nTwo \"builtin\" Source/Sink types that are included with the DataStreams package are the Data.Table and Data.RowTable types. Data.Table is a NamedTuple of AbstractVectors, with column names as NamedTuple fieldnames. This type supports both Data.Field and Data.Column streaming. Data.RowTable is just a Vector of NamedTuples, and as such, only supports Data.Field streaming.\n\nIn addition, any Data.Source can be iterated via the Data.rows(source) function, which returns a NamedTuple-iterator over the rows of a source. \n\n\n\n"
},

{
    "location": "index.html#Data.stream!-1",
    "page": "Home",
    "title": "Data.stream!",
    "category": "section",
    "text": "Data.stream!"
},

{
    "location": "index.html#DataStreams.Data.Schema",
    "page": "Home",
    "title": "DataStreams.Data.Schema",
    "category": "Type",
    "text": "A Data.Schema describes a tabular dataset, i.e. a set of named, typed columns with records as rows\n\nData.Schema allow Data.Source and Data.Sink to talk to each other and prepare to provide/receive data through streaming. Data.Schema provides the following accessible properties:\n\nData.header(schema) to return the header/column names in a Data.Schema\nData.types(schema) to return the column types in a Data.Schema; Union{T, Missing} indicates columns that may contain missing data (missing values)\nData.size(schema) to return the (# of rows, # of columns) in a Data.Schema; note that # of rows may be nothing, meaning unknown\n\nData.Schema has the following constructors:\n\nData.Schema(): create an \"emtpy\" schema with no rows, no columns, and no column names\nData.Schema(types[, header, rows, meta::Dict]): column element types are provided as a tuple or vector; column names provided as an iterable; # of rows can be an Int or missing to indicate unknown # of rows\n\nData.Schema are indexable via column names to get the number of that column in the Data.Schema\n\njulia> sch = Data.Schema([Int], [\"column1\"], 10)\nData.Schema:\nrows: 10	cols: 1\nColumns:\n \"column1\"  Int64\n\njulia> sch[\"column1\"]\n1\n\nDeveloper note: the full type definition is Data.Schema{R, T} where the R type parameter will be true or false, indicating whether the # of rows are known (i.e not missing), respectively. The T type parameter is a Tuple{A, B, ...} representing the column element types in the Data.Schema. Both of these type parameters provide valuable information that may be useful when constructing Sinks or streaming data.\n\n\n\n"
},

{
    "location": "index.html#Data.Schema-1",
    "page": "Home",
    "title": "Data.Schema",
    "category": "section",
    "text": "Data.SchemaThe reference DataStreams interface implementation lives in the DataStreams.jl package itself, implemented for a NamedTuple with AbstractVector elements.For examples of additional interface implementations, see some of the packages below:Data.Source implementations:CSV.Source\nSQLite.Source\nODBC.SourceData.Sink implementations:CSV.Sink\nSQLite.Sink\nODBC.Sink"
},

]}
