using Documenter, DataStreams

makedocs(
    modules = [DataStreams],
)

deploydocs(
    deps = Deps.pip("mkdocs", "python-markdown-math"),
    repo = "github.com/JuliaData/DataStreams.jl.git"
)
