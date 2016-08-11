using Documenter, DataStreams

makedocs(
    modules = [DataStreams],
    doctest = false
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
deploydocs(
    deps = Deps.pip(
            "pygments",
            "mkdocs",
            "mkdocs-material",
            "python-markdown-math"),
    repo = "github.com/JuliaData/DataStreams.jl.git"
)
