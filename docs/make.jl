using Documenter, DataStreams

makedocs(
    modules = [DataStreams],
)

deploydocs(
    deps = Deps.pip("mkdocs", "mkdocs-material", "python-markdown-math"),
    repo = "github.com/JuliaData/DataStreams.jl.git"
)
