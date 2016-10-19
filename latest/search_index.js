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
    "text": "The DataStreams.jl package aims to define a generic and performant framework for the transfer of \"table-like\" data. (i.e. data that can, at least in some sense, be described by rows and columns).The framework achieves this by defining interfaces (i.e. a group of methods) for Data.Source types and methods to describe how they \"provide\" data; as well as Data.Sink types and methods around how they \"receive\" data. This allows Data.Sources and Data.Sinks to implement their interfaces separately, without needing to be aware of each other. The end result is an ecosystem of packages that \"automatically\" talk with each other, with adding an additional package not requiring changes to existing packages.Packages can have a single julia type implement both the Data.Source and Data.Sink interfaces, or two separate types can implement them separately. For examples of interface implementations, see some of the packages below:Data.Source implementations:CSV.Source\nSQLite.Source\nDataFrame\nODBC.SourceData.Sink implementations:CSV.Sink\nSQLite.Sink\nDataFrame\nODBC.Sink"
},

{
    "location": "index.html#Data.Source-Interface-1",
    "page": "Home",
    "title": "Data.Source Interface",
    "category": "section",
    "text": "The Data.Source interface requires the following definitions, where MyPkg would represent a package wishing to implement the interface:Data.schema(::MyPkg.Source) => Data.Schema; get the Data.Schema of a Data.Source. Typically the Source type will store the Data.Schema directly, but this isn't strictly required. See ?Data.Schema or docs below for more information on Data.Schema\nData.isdone(::MyPkg.Source, row, col) => Bool; indicates whether the Data.Source will be able to provide a value at a given a row and col.Optional definition:Data.reference(::MyPkg.Source) => Vector{UInt8}; Sometimes, a Source needs the Sink to keep a reference to memory to keep a data structure valid. A Source can implement this method to return a Vector{UInt8} that the Sink will need to handle appropriately.\nBase.size(::MyPkg.Source[, i]) => Int; not explicitly required to enable data-streaming, but a Source should generally be able to describe its first 2 dimensions, i.e. # of rows and columns.A Data.Source also needs to \"register\" the type (or types) of streaming it supports. Currently defined streaming types in the DataStreams framework include:Data.Field: a field is the intersection of a specific row and column; this type of streaming will traverse the \"table\" structure by row, accessing each column on each row\nData.Column: this type of streaming will provide entire columns at a timeA Data.Source formally supports field-based streaming by defining the following:Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Field}) = true; declares that MyPkg.Source supports field-based streaming\nData.streamfrom{T}(::MyPkg.Source, ::Type{Data.Field}, ::Type{Nullable{T}}, row, col) => Nullable{T}; returns a value of type Nullable{T} given a specific row and col from MyPkg.Source\nData.streamfrom{T}(::MyPkg.Source, ::Type{Data.Field}, ::Type{T}, row, col) => T; returns a value of type T given a specific row and col from MyPkg.SourceAnd for column-based streaming:Data.streamtype(::Type{MyPkg.Source}, ::Type{Data.Column}) = true  \nData.streamfrom{T}(::Data.Source, ::Type{Data.Column}, ::Type{T}, col) => Vector{T}; Given a type T, returns column # col of a Data.Source as a Vector{T}\nData.streamfrom{T}(::Data.Source, ::Type{Data.Column}, ::Type{Nullable{T}}, col) => NullableVector{T}; Given a type Nullable{T}, returns column # col of a Data.Source as a NullableVector{T}"
},

{
    "location": "index.html#Data.Sink-Interface-1",
    "page": "Home",
    "title": "Data.Sink Interface",
    "category": "section",
    "text": "Similar to a Data.Source, a Data.Sink needs to \"register\" the types of streaming it supports, it does so through the following definition:Data.streamtypes(::Type{MyPkg.Sink}) = [Data.Field[, Data.Column]]; \"registers\" the streaming preferences for MyPkg.Sink. A Sink type should list the stream type or types it supports. If the Sink supports streaming of multiple types, it should list them in order of preference (i.e. the more natural or performant type first).A Data.Sink needs to also implement specific forms of constructors that ensure proper Sink state in many higher-level streaming functions:MyPkg.Sink{T <: Data.StreamType}(schema::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8}, args...; kwargs...); given a schema::Data.Schema a source will be providing, the type of streaming T (Field or Column), whether the user desires to append the data or not, a possible memory reference ref and any necessary args... and kwargs..., construct an appropriate instance of MyPkg.Sink ready to receive data from source. The append argument allows an already existing sink file/source to \"reset\" itself if the user does not desire to append.\nMyPkg.Sink{T <: Data.StreamType}(sink, schema::Data.Schema, ::Type{T}, append::Bool, ref::Vector{UInt8}); similar to above, but instead of constructing a new Sink, an existing Sink is given as a first argument, which may be modified before being returned, ready to receive data according to the Data.Source schema.Similar to Data.Source, a Data.Sink also needs to implement it's own streamto! method that indicates how it receives data.A Data.Sink supports field-based streaming by defining:Data.streamto!{T}(sink::MyPkg.Sink, ::Type{Data.Field}, val::T, row, col[, schema]): Given a row, col, and val::T a Data.Sink should store the value appropriately. The type of the value retrieved is given by T, which may be Nullable{T}. Optionally provided is the schema (the same schema that is passed in the MyPkg.Sink(schema, ...) constructors). This argument is passed for efficiency sinceit can be calculated once at the beginning of a Data.stream! and used quickly for many calls to Data.streamto!. This argument is optional, because a Sink can overload Data.streamto! with or without it.A Data.Sink supports column-based streaming by defining:* `Data.streamto!{T}(sink::MyPkg.Sink, ::Type{Data.Column}, column::Type{T}, row, col[, schema])`: Given a column number `col` and column of data `column`, a `Data.Sink` should store it appropriately. The type of the column is given by `T`, which may be a `NullableVector{T}`. Optionally provided is the `schema` (the same `schema` that is passed in the `MyPkg.Sink(schema, ...)` constructors). This argument is passed for efficiency since it can be calculated once at the beginning of a `Data.stream!` and used quickly for many calls to `Data.streamto!`. This argument is optional, because a Sink can overload `Data.streamto!` with or without it.A Data.Sink can optionally define the following if needed:Data.cleanup!(sink::MyPkg.Sink): certain Data.Sink, like databases, may need to protect against inconvenient or dangerous \"states\" if there happens to be an error while streaming. Data.cleanup! provides the sink a way to rollback a transaction or other kind of cleanup if an error occurs during streaming\nData.close!(sink::MyPkg.Sink): during the Data.stream! workflow, a Data.Sink should remain \"open\" to receiving data until Data.close! is call explicitly. Data.close! is defined to allow a sink to fully commit all streaming results and close/destroy any necessary resources."
},

{
    "location": "index.html#DataStreams.Data.Schema",
    "page": "Home",
    "title": "DataStreams.Data.Schema",
    "category": "Type",
    "text": "A Data.Schema describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows)\n\nData.Schema allow Data.Source and Data.Sink to talk to each other and prepare to provide/receive data through streaming. Data.Schema fields include:\n\nA boolean type parameter that indicates whether the # of rows is known in the Data.Source; this is useful as a type parameter to allow Data.Sink and Data.streamto! methods to dispatch. Note that the sentinel value -1 is used as the # of rows when the # of rows is unknown.\nData.header(schema) to return the header/column names in a Data.Schema\nData.types(schema) to return the column types in a Data.Schema; Nullable{T} indicates columns that may contain missing data (null values)\nData.size(schema) to return the (# of rows, # of columns) in a Data.Schema\n\nData.Source and Data.Sink interfaces both require that Data.schema(source_or_sink) be defined to ensure that other Data.Source/Data.Sink can work appropriately.\n\n\n\n"
},

{
    "location": "index.html#Data.Schema-1",
    "page": "Home",
    "title": "Data.Schema",
    "category": "section",
    "text": "Data.Schema"
},

]}
