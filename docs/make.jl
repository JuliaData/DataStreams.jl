using Documenter, DataStreams

makedocs(
    modules = [DataStreams],
    format = :html,
    sitename = "DataStreams.jl",
    pages = ["Home" => "index.md"]
)

deploydocs(
    repo = "github.com/JuliaData/DataStreams.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
    julia = "nightly",
    osname = "linux"
)
