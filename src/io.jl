
Base.show{T}(io::IO, ::Type{PointerString{T}}) = print(io, "PointerString{$T}")
Base.show(io::IO, x::PointerString{UInt16}) = print(io, x == NULLSTRING16 ? "PointerString(\"\")" : "PointerString(\"$(utf16(x.ptr, x.len))\")")
Base.show(io::IO, x::PointerString{UInt32}) = print(io, x == NULLSTRING32 ? "PointerString(\"\")" : "PointerString(\"$(utf32(x.ptr, x.len))\")")
Base.showcompact(io::IO, x::PointerString{UInt16}) = print(io, x == NULLSTRING16 ? "\"\"" : "\"$(utf16(x.ptr, x.len))\"")
Base.showcompact(io::IO, x::PointerString{UInt32}) = print(io, x == NULLSTRING32 ? "\"\"" : "\"$(utf32(x.ptr, x.len))\"")
Base.string(x::PointerString{UInt16}) = x == NULLSTRING16 ? utf16("") : utf16(x.ptr, x.len)
Base.string(x::PointerString{UInt32}) = x == NULLSTRING32 ? utf32("") : utf32(x.ptr, x.len)
Base.convert(::Type{Compat.UTF8String}, x::PointerString) = convert(Compat.UTF8String, string(x))
Base.convert(::Type{UTF16String}, x::PointerString) = convert(UTF16String, string(x))
Base.convert(::Type{UTF32String}, x::PointerString) = convert(UTF32String, string(x))
Base.convert(::Type{PointerString{UInt8}}, x::Compat.UTF8String) = PointerString(pointer(x.data), length(x))
Base.convert(::Type{PointerString{UInt16}}, x::UTF16String) = PointerString(pointer(x.data), length(x))
Base.convert(::Type{PointerString{UInt32}}, x::UTF32String) = PointerString(pointer(x.data), length(x))

if VERSION < v"0.5.0-dev"
    Base.convert(::Type{ASCIIString}, x::PointerString) = convert(ASCIIString, string(x))
    Base.convert(::Type{PointerString{UInt8}}, x::ASCIIString) = PointerString(pointer(x.data), length(x))
    Base.string(x::PointerString) = x == NULLSTRING ? Compat.UTF8String("") : utf8(x.ptr, x.len)
	Base.show(io::IO, x::PointerString) = print(io, x == NULLSTRING ? "PointerString(\"\")" : "PointerString(\"$(utf8(x.ptr, x.len))\")")
	Base.showcompact(io::IO, x::PointerString) = print(io, x == NULLSTRING ? "\"\"" : "\"$(utf8(x.ptr, x.len))\"")
else
	Base.string(x::PointerString) = x == NULLSTRING ? Compat.UTF8String("") : Compat.UTF8String(x.ptr, x.len)
	Base.show(io::IO, x::PointerString) = print(io, x == NULLSTRING ? "PointerString(\"\")" : "PointerString(\"$(Compat.UTF8String(x.ptr, x.len))\")")
	Base.showcompact(io::IO, x::PointerString) = print(io, x == NULLSTRING ? "\"\"" : "\"$(Compat.UTF8String(x.ptr, x.len))\"")
end

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
    cols = size(schema, 2)
    println(io, "$(schema.rows)x$(schema.cols) Data.Schema:")
    max_col_lens = [min(div(MAX_COLUMN_WIDTH,2), length(nm)) for nm in nms]
    max_col_lens = [max(max_col_lens[i], length(string(typs[i])))+1 for i = 1:cols]
    upper = min(MAX_NUM_OF_COLS_TO_PRINT, cols)
    cant_print_all = MAX_NUM_OF_COLS_TO_PRINT < cols
    for i = 1:upper
        nm = nms[i]
        print(io, length(nm) > max_col_lens[i] ? string(nm[1:chr2ind(nm, max_col_lens[i]-3)],"...") : lpad(nm, max_col_lens[i], ' '), ifelse(i == upper, ifelse(cant_print_all," ...\n","\n"), ","))
    end
    for i = 1:upper
        print(io, lpad(string(typs[i]), max_col_lens[i], ' '), ifelse(i == upper, ifelse(cant_print_all," ...\n","\n"), ","))
    end
end
