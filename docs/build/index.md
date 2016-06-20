
<a id='DataStreams.jl-1'></a>

# DataStreams.jl

<a id='DataStreams' href='#DataStreams'>#</a>
**`DataStreams`** &mdash; *Module*.



The `DataStreams.jl` package defines a data processing framework based on Sources, Sinks, and the `Data.stream!` function.

`DataStreams` defines the common infrastructure leveraged by individual packages to create systems of various data sources and sinks that talk to each other in a unified, consistent way.

The workflow enabled by the `DataStreams` framework involves:

  * constructing new `Source` types to allow streaming data from files, databases, etc.
  * `Data.stream!` those datasets to newly created or existing `Sink` types
  * convert `Sink` types that have received data into new `Source` types
  * continue to `Data.stream!` from `Source`s to `Sink`s

The typical approach for a new package to "satisfy" the DataStreams interface is to:

  * Define a `Source` type that wraps an "true data source" (i.e. a file, database table/query, etc.) and fulfills the `Source` interface (see `?Data.Source`)
  * Define a `Sink` type that can create or write data to an "true data source" and fulfills the `Sink` interface (see `?Data.Sink`)
  * Define appropriate `Data.stream!(::Source, ::Sink)` methods as needed between various combinations of Sources and Sinks;    i.e. define `Data.stream!(::NewPackage.Source, ::CSV.Sink)` and `Data.stream!(::CSV.Source, ::NewPackage.Sink)`


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L2-21' class='documenter-source'>source</a><br>

<a id='DataStreams.Data.Schema' href='#DataStreams.Data.Schema'>#</a>
**`DataStreams.Data.Schema`** &mdash; *Type*.



A `Data.Schema` describes a tabular dataset (i.e. a set of optionally named, typed columns with records as rows) Access to `Data.Schema` fields includes:

  * `Data.header(schema)` to return the header/column names in a `Data.Schema`
  * `Data.types(schema)` to return the column types in a `Data.Schema`
  * `Data.size(schema)` to return the (# of rows, # of columns) in a `Data.Schema`


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L109-116' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">Schema</span><span class="p">(</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L134">src/DataStreams.jl:134</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">Schema</span><span class="p">(</span><span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L132">src/DataStreams.jl:132</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">header</span><span class="p">::</span><span class="n">Array{T<:Any,1}</span><span class="p">,
</span>    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L124">src/DataStreams.jl:124</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">header</span><span class="p">::</span><span class="n">Array{T<:Any,1}</span><span class="p">,
</span>    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span><span class="p">,
</span>    <span class="n">rows</span><span class="p">::</span><span class="n">Integer</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L124">src/DataStreams.jl:124</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">header</span><span class="p">::</span><span class="n">Array{T<:Any,1}</span><span class="p">,
</span>    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span><span class="p">,
</span>    <span class="n">rows</span><span class="p">::</span><span class="n">Integer</span><span class="p">,
</span>    <span class="n">metadata</span><span class="p">::</span><span class="n">Dict</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L124">src/DataStreams.jl:124</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">header</span><span class="p">,
</span>    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L131">src/DataStreams.jl:131</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">header</span><span class="p">,
</span>    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span><span class="p">,
</span>    <span class="n">rows</span><span class="p">::</span><span class="n">Integer</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L131">src/DataStreams.jl:131</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">header</span><span class="p">,
</span>    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span><span class="p">,
</span>    <span class="n">rows</span><span class="p">::</span><span class="n">Integer</span><span class="p">,
</span>    <span class="n">meta</span><span class="p">::</span><span class="n">Dict</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L131">src/DataStreams.jl:131</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span><span class="p">,
</span>    <span class="n">rows</span><span class="p">::</span><span class="n">Integer</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L132">src/DataStreams.jl:132</a>
</li>
<li>
    <pre><span class="nf">Schema</span><span class="p">(</span>
    <span class="n">types</span><span class="p">::</span><span class="n">Array{DataType,1}</span><span class="p">,
</span>    <span class="n">rows</span><span class="p">::</span><span class="n">Integer</span><span class="p">,
</span>    <span class="n">meta</span><span class="p">::</span><span class="n">Dict</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L132">src/DataStreams.jl:132</a>
</li>
</ul>

_Hiding 1 method defined outside of this package._

<a id='DataStreams.Data.Sink' href='#DataStreams.Data.Sink'>#</a>
**`DataStreams.Data.Sink`** &mdash; *Type*.



A `Data.Sink` type represents a data destination; i.e. an "true data source" such as a database, file, API endpoint, etc.

There are two broad types of `Sink`s:

1. "new sinks": an independent `Sink` constructor creates a *new* "true data source" that can be streamed to
2. "existing sinks": the `Sink` wraps an already existing "true data source" (or `Source` object that wraps an "true data source").     Upon construction of these `Sink`s, there is no new creation of "true data source"s; the "ulitmate data source" is simply wrapped to replace or append to

`Sink`s also have notions of state:

  * `BEGINNING`: the `Sink` is freshly constructed and ready to stream data to; this includes initial metadata like column headers
  * `WRITING`: data has been streamed to the `Sink`, but is still open to receive more data
  * `DONE`: the `Sink` has been closed and can no longer receive data

The `Data.Sink` interface includes the following:

  * `Data.schema(::Data.Sink) => Data.Schema`; typically the `Sink` type will store the `Data.Schema` directly, but this isn't strictly required


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L80-98' class='documenter-source'>source</a><br>

<a id='DataStreams.Data.Source' href='#DataStreams.Data.Source'>#</a>
**`DataStreams.Data.Source`** &mdash; *Type*.



A `Data.Source` type holds data that can be read/queried/parsed/viewed/streamed; i.e. a "true data source" To clarify, there are two distinct types of "source":

1. the "true data source", which would be the file, database, API, structure, etc; i.e. the actual data
2. the `Data.Source` julia object that wraps the "true source" and provides the `DataStreams` interface

`Source` types have two different types of constructors:

1. "independent constructors" that wrap "true data sources"
2. "sink constructors" where a `Data.Sink` object that has received data is turned into a new `Source` (useful for chaining data processing tasks)

`Source`s also have a, currently implicit, notion of state:

  * `BEGINNING`: a `Source` is in this state immediately after being constructed and is ready to be used; i.e. ready to read/parse/query/stream data from it
  * `READING`: the ingestion of data from this `Source` has started and has not finished yet
  * `DONE`: the ingestion process has exhausted all data expected from this `Source`

The `Data.Source` interface includes the following:

  * `Data.schema(::Data.Source) => Data.Schema`; typically the `Source` type will store the `Data.Schema` directly, but this isn't strictly required
  * `Data.reset!(::Data.Source)`; used to reset a `Source` type from `READING` or `DONE` to the `BEGINNING` state, ready to be read from again
  * `eof(::Data.Source)`; indicates whether the `Source` type is in the `DONE` state; i.e. all data has been exhausted from this source


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L32-56' class='documenter-source'>source</a><br>

<a id='Base.size' href='#Base.size'>#</a>
**`Base.size`** &mdash; *Function*.



Returns the (# of rows,# of columns) associated with a specific `Source` or `Sink`


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L160' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">size</span><span class="p">(</span><span class="n">sch</span><span class="p">::</span><span class="n">DataStreams.Data.Schema</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L138">src/DataStreams.jl:138</a>
</li>
<li>
    <pre><span class="nf">size</span><span class="p">(</span>
    <span class="n">sch</span><span class="p">::</span><span class="n">DataStreams.Data.Schema</span><span class="p">,
</span>    <span class="n">i</span><span class="p">::</span><span class="n">Int64</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L139">src/DataStreams.jl:139</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">size</span><span class="p">(</span><span class="n">io</span><span class="p">::</span><span class="n">DataStreams.Data.Source</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L161">src/DataStreams.jl:161</a>
</li>
<li>
    <pre><span class="nf">size</span><span class="p">(</span>
    <span class="n">io</span><span class="p">::</span><span class="n">DataStreams.Data.Source</span><span class="p">,
</span>    <span class="n">i</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L162">src/DataStreams.jl:162</a>
</li>
</ul>

_Hiding 99 methods defined outside of this package._

<a id='DataStreams.Data.header' href='#DataStreams.Data.header'>#</a>
**`DataStreams.Data.header`** &mdash; *Function*.



Returns the header/column names (if any) associated with a specific `Source` or `Sink`


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L156' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">header</span><span class="p">(</span><span class="n">sch</span><span class="p">::</span><span class="n">DataStreams.Data.Schema</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L136">src/DataStreams.jl:136</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">header</span><span class="p">(</span><span class="n">io</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L157">src/DataStreams.jl:157</a>
</li>
</ul>

<a id='DataStreams.Data.schema' href='#DataStreams.Data.schema'>#</a>
**`DataStreams.Data.schema`** &mdash; *Function*.



Returns the `Data.Schema` for `io`


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L154' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">schema</span><span class="p">(</span><span class="n">df</span><span class="p">::</span><span class="n">DataFrames.DataFrame</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L175">src/DataStreams.jl:175</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">schema</span><span class="p">(</span><span class="n">io</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L155">src/DataStreams.jl:155</a>
</li>
</ul>

<a id='DataStreams.Data.stream!' href='#DataStreams.Data.stream!'>#</a>
**`DataStreams.Data.stream!`** &mdash; *Function*.



`Data.stream!(::Data.Source, ::Data.Sink)` starts transfering data from a newly constructed `Source` type to a newly constructed `Sink` type. Data transfer typically continues until `eof(source) == true`, i.e. the `Source` is exhausted, at which point the `Sink` is closed and may no longer receive data. See individual `Data.stream!` methods for more details on specific `Source`/`Sink` combinations.


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L101-105' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre><span class="nf">stream!</span><span class="p">{</span><span class="n">T<:DataStreams.Data.Sink</span><span class="p">}</span><span class="p">(</span>
    <span class="n">source</span><span class="p">::</span><span class="n">DataStreams.Data.Source</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{T}</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L167">src/DataStreams.jl:167</a>
</li>
<li>
    <pre><span class="nf">stream!</span><span class="p">(</span>
    <span class="n">source</span><span class="p">::</span><span class="n">DataStreams.Data.Source</span><span class="p">,
</span>    <span class="n"></span><span class="p">::</span><span class="n">Type{DataFrames.DataFrame}</span>
<span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L191">src/DataStreams.jl:191</a>
</li>
</ul>

<a id='DataStreams.Data.types' href='#DataStreams.Data.types'>#</a>
**`DataStreams.Data.types`** &mdash; *Function*.



Returns the column types associated with a specific `Source` or `Sink`


<a target='_blank' href='https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L158' class='documenter-source'>source</a><br>

<strong>Methods</strong>

<ul class="documenter-methodtable">
<li>
    <pre class="documenter-inline"><span class="nf">types</span><span class="p">(</span><span class="n">sch</span><span class="p">::</span><span class="n">DataStreams.Data.Schema</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L137">src/DataStreams.jl:137</a>
</li>
<li>
    <pre class="documenter-inline"><span class="nf">types</span><span class="p">(</span><span class="n">io</span><span class="p">)</span></pre>
    defined at
    <a target="_blank" href="https://github.com/JuliaDB/DataStreams.jl/tree/133cccdf6a2515f3e881d58a16f706606dd7b550/src/DataStreams.jl#L159">src/DataStreams.jl:159</a>
</li>
</ul>

