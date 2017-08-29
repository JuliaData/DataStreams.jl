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
    "text": "Data.streamtypes(::Type{[Sink]}) => Vector{StreamType}\n\nReturns a vector of Data.StreamTypes that the sink is able to receive; the order of elements indicates the sink's streaming preference\n\nFor example, if my sink only supports Data.Field streaming, I would simply define:\n\nData.streamtypes(::Type{MyPkg.Sink}) = [Data.Field]\n\nIf, on the other hand, my sink also supported Data.Column streaming, and Data.Column streaming happend to be more efficient, I could define:\n\nData.streamtypes(::Type{MyPkg.Sink}) = [Data.Column, Data.Field] # put Data.Column first to indicate preference\n\n\n\n"
},

{
    "location": "index.html#DataStreams.Data.streamto!",
    "page": "Home",
    "title": "DataStreams.Data.streamto!",
    "category": "Function",
    "text": "Data.streamto!(sink, S::Type{StreamType}, val, row, col) Data.streamto!(sink, S::Type{StreamType}, val, row, col, knownrows)\n\nStreams data to a sink. S is the type of streaming (Data.Field or Data.Column). val is the value (single field or column) to be streamed to the sink. row and col indicate where the data should be streamed/stored.\n\nA sink may optionally define the method that also accepts the knownrows argument, which will be true or false, indicating whether the source streaming has a known # of rows or not. This can be useful for sinks that may know how to pre-allocate space in the cases where the source can tell the # of rows, or in the case of unknown # of rows, may need to stream the data in differently.\n\n\n\n"
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
    "text": "Data.stream!(source, sink; append::Bool=false, transforms=Dict()) Data.stream!(source, ::Type{Sink}, args...; append::Bool=false, transforms=Dict(), kwargs...)\n\nStream data from source to sink. The 1st definition assumes already constructed source & sink and takes two optional keyword arguments:\n\nappend::Bool=false: whether the data from source should be appended to sink\ntransforms::Dict: A dict with mappings between column # or name (Int or String) to a \"transform\" function. For Data.Field streaming, the transform function should be of the form f(x::T) => y::S, i.e. takes a single input of type T and returns a single value of type S. For Data.Column streaming, it should be of the form f(x::AbstractVector{T}) => y::AbstractVector{S}, i.e. take an AbstractVector with eltype T and return another AbstractVector with eltype S.\n\nFor the 2nd definition, the Sink type itself is passed as the 2nd argument (::Type{Sink}) and is constructed \"on-the-fly\", being passed args... and kwargs... like Sink(args...; kwargs...).\n\nWhile users are free to call Data.stream! themselves, oftentimes, packages want to provide even higher-level convenience functions.\n\nAn example of of these higher-level convenience functions are from CSV.jl:\n\nfunction CSV.read(fullpath::Union{AbstractString,IO}, sink::Type=DataFrame, args...; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)\n    source = CSV.Source(fullpath; kwargs...)\n    sink = Data.stream!(source, sink, args...; append=append, transforms=transforms, kwargs...)\n    return Data.close!(sink)\nend\n\nfunction CSV.read{T}(fullpath::Union{AbstractString,IO}, sink::T; append::Bool=false, transforms::Dict=Dict{Int,Function}(), kwargs...)\n    source = CSV.Source(fullpath; kwargs...)\n    sink = Data.stream!(source, sink; append=append, transforms=transforms)\n    return Data.close!(sink)\nend\n\nIn this example, CSV.jl defines it's own high-level function for reading from a CSV.Source. In these examples, a CSV.Source is constructed using the fullpath argument, along w/ any extra kwargs.... The sink can be provided as a type with args... and kwargs... that will be passed to its DataStreams constructor, like Sink(sch, streamtype, append, args...; kwargs...); otherwise, an already-constructed Sink can be provided directly (2nd example).\n\nOnce the source is constructed, the data is streamed via the call to Data.stream(source, sink; append=append, transforms=transforms), with the sink being returned.\n\nAnd finally, to \"finish\" the streaming process, Data.close!(sink) is closed, which returns the finalized sink. Note that Data.stream!(source, sink) could be called multiple times with different sources and the same sink, most likely with append=true being passed, to enable the accumulation of several sources into a single sink. A single Data.close!(sink) method should be called to officially close or commit the final sink.\n\n\n\n"
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
    "text": "A Data.Schema describes a tabular dataset, i.e. a set of named, typed columns with records as rows\n\nData.Schema allow Data.Source and Data.Sink to talk to each other and prepare to provide/receive data through streaming. Data.Schema provides the following accessible properties:\n\nData.header(schema) to return the header/column names in a Data.Schema\nData.types(schema) to return the column types in a Data.Schema; Nullable{T} indicates columns that may contain missing data (null values)\nData.size(schema) to return the (# of rows, # of columns) in a Data.Schema; note that # of rows may be null, meaning unknown\n\nData.Schema has the following constructors:\n\nData.Schema(): create an \"emtpy\" schema with no rows, no columns, and no column names\nData.Schema(types[, header, rows, meta::Dict]): column element types are provided as a tuple or vector; column names provided as an iterable; # of rows can be an Int or null to indicate unknown # of rows\n\nData.Schema are indexable via column names to get the number of that column in the Data.Schema\n\njulia> sch = Data.Schema([\"column1\"], [Int], 10)\nData.Schema:\nrows: 10	cols: 1\nColumns:\n \"column1\"  Int64\n\njulia> sch[\"column1\"]\n1\n\nDeveloper note: the full type definition is Data.Schema{R, T} where the R type parameter will be true or false, indicating whether the # of rows are known (i.e not null), respectively. The T type parameter is a Tuple{A, B, ...} representing the column element types in the Data.Schema. Both of these type parameters provide valuable information that may be useful when constructing Sinks or streaming data.\n\n\n\n"
},

{
    "location": "index.html#Data.Schema-1",
    "page": "Home",
    "title": "Data.Schema",
    "category": "section",
    "text": "Data.SchemaThe reference DataStreams interface implementation lives in the DataStreams.jl package itself, implemented for a NamedTuple with AbstractVector elements.For examples of additional interface implementations, see some of the packages below:Data.Source implementations:CSV.Source\nSQLite.Source\nODBC.SourceData.Sink implementations:CSV.Sink\nSQLite.Sink\nODBC.Sink"
},

]}
