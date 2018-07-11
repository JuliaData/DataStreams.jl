using DataStreams, Test

df = (a=[1,2,3,1], b=["hey", "ho", "neighbor", "hi"], c=[4.0, 5.0, 6.6, 6.0], d=[0,0,0,0])

# sink = Data.Table
cell(x::Data.Table, row, col) = x[col][row]
cell(x::Data.RowTable, row, col) = x[row][col]

@testset "Data.query" begin
for sink in (Data.Table, Data.RowTable)
    # select *
    res = Data.query(df, [(col=1,), (col=2,), (col=3,), (col=4,)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String, Float64, Int)
    @test Data.header(sch) == ["a", "b", "c", "d"]
    @test size(sch) == (4, 4)
    @test cell(res, 1, 1) == 1
    @test cell(res, 1, 4) == 0
    @test cell(res, 4, 1) == 1
    @test cell(res, 4, 4) == 0

    # select *
    res = Data.query(df)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String, Float64, Int)
    @test Data.header(sch) == ["a", "b", "c", "d"]
    @test size(sch) == (4, 4)
    @test cell(res, 1, 1) == 1
    @test cell(res, 1, 4) == 0
    @test cell(res, 4, 1) == 1
    @test cell(res, 4, 4) == 0

    # select * offset 1
    res = Data.query(df, [(col=1,), (col=2,), (col=3,), (col=4,)], sink, offset=1)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String, Float64, Int)
    @test Data.header(sch) == ["a", "b", "c", "d"]
    @test size(sch) == (3, 4)

    # select * offset 1 limit 1
    res = Data.query(df, [(col=1,), (col=2,), (col=3,), (col=4,)], sink, offset=1, limit=1)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String, Float64, Int)
    @test Data.header(sch) == ["a", "b", "c", "d"]
    @test size(sch) == (1, 4)

    # select * offset 1 limit 5
    res = Data.query(df, [(col=1,), (col=2,), (col=3,), (col=4,)], sink, offset=1, limit=5)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String, Float64, Int)
    @test Data.header(sch) == ["a", "b", "c", "d"]
    @test size(sch) == (3, 4)

    # select * offset 4 limit 5
    res = Data.query(df, [(col=1,), (col=2,), (col=3,), (col=4,)], sink, offset=4, limit=5)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String, Float64, Int)
    @test Data.header(sch) == ["a", "b", "c", "d"]
    @test size(sch) == (0, 4)

    # select * where a = 2
    res = Data.query(df, [(col=1, filter=x->x==2), (col=2,), (col=3,)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String, Float64)
    @test Data.header(sch) == ["a", "b", "c"]
    @test size(sch) == (1, 3)

    # select a, b where a = 2
    res = Data.query(df, [(col=1, filter=x->x==2), (col=2,)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String)
    @test Data.header(sch) == ["a", "b"]
    @test size(sch) == (1, 2)

    # select b, a where a = 2
    res = Data.query(df, [(col=2,), (col=1, filter=x->x==2)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (String, Int)
    @test Data.header(sch) == ["b", "a"]
    @test size(sch) == (1, 2)

    # select c
    res = Data.query(df, [(col=3,)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Float64,)
    @test Data.header(sch) == ["c"]
    @test size(sch) == (4, 1)

    # select c where c < 6.0
    res = Data.query(df, [(col=3, filter=x->x < 6.0)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Float64,)
    @test Data.header(sch) == ["c"]
    @test size(sch) == (2, 1)

    # select a + 1
    res = Data.query(df, [(compute=x->x +1, computeargs=(1,))], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int,)
    @test Data.header(sch) == ["Column1"]
    @test size(sch) == (4, 1)

    # select c * 2.0 as c where c < 6.0
    res = Data.query(df, [(col=3, hide=true, filter=x->x < 6.0), (name=:c, compute=x->x * 2.0, computeargs=(3,))], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Float64,)
    @test Data.header(sch) == ["c"]
    @test size(sch) == (2, 1)

    # select c * 2.0 as c, c / 2.0 as c2 where c < 6.0
    res = Data.query(df, [(col=3, hide=true, filter=x->x < 6.0),
                    (name=:c, compute=x->x * 2.0, computeargs=(3,)),
                    (name=:c2, compute=x->x / 2.0, computeargs=(3,))], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Float64, Float64)
    @test Data.header(sch) == ["c", "c2"]
    @test size(sch) == (2, 2)

    # select a, sum(c) group by a
    res = Data.query(df, [(col=1, group=true), (col=3, aggregate=sum)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "c"]
    @test size(sch) == (3, 2)

    # select a, sum(c - d) group by a
    res = Data.query(df, [(col=1, group=true), (name=:c, compute=(x,y)->x - y, computeargs=(3,4), aggregate=sum)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "c"]
    @test size(sch) == (3, 2)

    # select a group by a
    res = Data.query(df, [(col=1, group=true)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int,)
    @test Data.header(sch) == ["a"]
    @test size(sch) == (3, 1)

    # select a, SUM(c) + SUM(d) as f group by a
    res = Data.query(df, [(col=1, group=true), (name=:f, computeaggregate=(c, d)->sum(c) + sum(d), computeargs=(3, 4))], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "f"]
    @test size(sch) == (3, 2)

    # select a, SUM(c) + SUM(d) as f where b like 'h%' group by a
    res = Data.query(df, [(col=1, group=true), (col=2, filter=x->x[1] == 'h', hide=true), (name=:f, computeaggregate=(c, d)->sum(c) + sum(d), computeargs=(3, 4))], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "f"]
    @test size(sch) == (2, 2)

    # select a, sum(c) group by a having sum(c) < 7.0
    res = Data.query(df, [(col=1, group=true), (col=3, aggregate=sum, having=x->x < 7.0)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "c"]
    @test size(sch) == (2, 2)

    # select a order by a
    res = Data.query(df, [(col=1, sort=true)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int,)
    @test Data.header(sch) == ["a"]
    @test size(sch) == (4, 1)

    # select a order by a desc
    res = Data.query(df, [(col=1, sort=true, sortasc=false)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int,)
    @test Data.header(sch) == ["a"]
    @test size(sch) == (4, 1)

    # select a, b order by a, b
    res = Data.query(df, [(col=1, sort=true), (col=2, sort=true)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String)
    @test Data.header(sch) == ["a", "b"]
    @test size(sch) == (4, 2)

    # select a, b order by a, b desc
    res = Data.query(df, [(col=1, sort=true), (col=2, sort=true, sortasc=false)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String)
    @test Data.header(sch) == ["a", "b"]
    @test size(sch) == (4, 2)

    # select a, b order by a desc, b desc
    res = Data.query(df, [(col=1, sort=true, sortasc=false), (col=2, sort=true, sortasc=false)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String)
    @test Data.header(sch) == ["a", "b"]
    @test size(sch) == (4, 2)

    # select a, b order by a desc, b asc
    res = Data.query(df, [(col=1, sort=true, sortasc=false), (col=2, sort=true)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String)
    @test Data.header(sch) == ["a", "b"]
    @test size(sch) == (4, 2)

    # select a, b order by b, a
    res = Data.query(df, [(col=1, sort=true, sortindex=2, sortasc=false), (col=2, sort=true, sortindex=1)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String)
    @test Data.header(sch) == ["a", "b"]
    @test size(sch) == (4, 2)

    # select a, b where a = 1 order by b desc, a
    res = Data.query(df, [(col=1, filter=x->x==1, sort=true, sortindex=2), (col=2, sort=true, sortindex=1, sortasc=false)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, String)
    @test Data.header(sch) == ["a", "b"]
    @test size(sch) == (2, 2)

    # select a, sum(c) group by a
    res = Data.query(df, [(col=1, group=true, sort=true), (col=3, aggregate=sum)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "c"]
    @test size(sch) == (3, 2)

    # select a, sum(c) group by a order by a having sum(c) < 7.0
    res = Data.query(df, [(col=1, group=true, sort=true), (col=3, aggregate=sum, having=x->x < 7.0)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "c"]
    @test size(sch) == (2, 2)

    # select a, sum(c) group by a order by b desc having sum(c) < 7.0
    res = Data.query(df, [(col=1, group=true), (col=3, aggregate=sum, sort=true, sortasc=false, having=x->x < 7.0)], sink)
    sch = Data.schema(res)
    @test Data.types(sch) == (Int, Float64)
    @test Data.header(sch) == ["a", "c"]
    @test size(sch) == (2, 2)
end
end # testset "Data.query"
