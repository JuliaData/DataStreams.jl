using DataStreams
df = (a=[1,2,3, 1], b=["hey", "ho", "neighbor", "hi"], c=[4.0, 5.0, 6.6, 6.0], d=[0,0,0,0])
sink = Data.RowTable

for sink in (Data.Table, Data.RowTable)
    # select *
    res = Data.query(df, [(col=1,), (col=2,), (col=3,), (col=4,)], sink)

    # select * where a = 2
    res = Data.query(df, [(col=1, filter=x->x==2), (col=2,), (col=3,)], sink)

    # select a, b where a = 2
    res = Data.query(df, [(col=1, filter=x->x==2), (col=2,)], sink)

    # select b, a where a = 2
    res = Data.query(df, [(col=2,), (col=1, filter=x->x==2)], sink)

    # select c
    res = Data.query(df, [(col=3,)], sink)

    # select c where c < 6.0
    res = Data.query(df, [(col=3, filter=x->x < 6.0)], sink)

    # select a + 1
    res = Data.query(df, [(compute=x->x +1, computeargs=(1,))], sink)

    # select c * 2.0 as c where c < 6.0
    res = Data.query(df, [(col=3, hide=true, filter=x->x < 6.0), (name=:c, compute=x->x * 2.0, computeargs=(3,))], sink)

    # select c * 2.0 as c, c / 2.0 as c2 where c < 6.0
    res = Data.query(df, [(col=3, hide=true, filter=x->x < 6.0),
                    (name=:c, compute=x->x * 2.0, computeargs=(3,)),
                    (name=:c2, compute=x->x / 2.0, computeargs=(3,))], sink)

    # select a, sum(c) group by a
    res = Data.query(df, [(col=1, group=true), (col=3, aggregate=sum)], sink)

    # select a group by a
    res = Data.query(df, [(col=1, group=true)], sink)

    # select a, SUM(c) + SUM(d) as f group by a
    res = Data.query(df, [(col=1, group=true), (name=:f, computeaggregate=(c, d)->sum(c) + sum(d), computeargs=(3, 4))], sink)

    # select a, SUM(c) + SUM(d) as f where b like 'h%' group by a
    res = Data.query(df, [(col=1, group=true), (col=2, filter=x->x[1] == 'h', hide=true), (name=:f, computeaggregate=(c, d)->sum(c) + sum(d), computeargs=(3, 4))], sink)

    # select a, sum(c) group by a having sum(c) < 7.0
    res = Data.query(df, [(col=1, group=true), (col=3, aggregate=sum, having=x->x < 7.0)], sink)

    # select a order by a
    res = Data.query(df, [(col=1, sort=true)], sink)

    # select a order by a desc
    res = Data.query(df, [(col=1, sort=true, sortasc=false)], sink)

    # select a, b order by a, b
    res = Data.query(df, [(col=1, sort=true), (col=2, sort=true)], sink)

    # select a, b order by a, b desc
    res = Data.query(df, [(col=1, sort=true), (col=2, sort=true, sortasc=false)], sink)

    # select a, b order by a desc, b desc
    res = Data.query(df, [(col=1, sort=true, sortasc=false), (col=2, sort=true, sortasc=false)], sink)

    # select a, b order by a desc, b asc
    res = Data.query(df, [(col=1, sort=true, sortasc=false), (col=2, sort=true)], sink)

    # select a, b order by b, a
    res = Data.query(df, [(col=1, sort=true, sortindex=2, sortasc=false), (col=2, sort=true, sortindex=1)], sink)

    # select a, b where a = 1 order by b desc, a
    res = Data.query(df, [(col=1, filter=x->x==1, sort=true, sortindex=2), (col=2, sort=true, sortindex=1, sortasc=false)], sink)

    # select a, sum(c) group by a
    res = Data.query(df, [(col=1, group=true, sort=true), (col=3, aggregate=sum)], sink)

    # select a, sum(c) group by a order by a having sum(c) < 7.0
    res = Data.query(df, [(col=1, group=true, sort=true), (col=3, aggregate=sum, having=x->x < 7.0)], sink)

    # select a, sum(c) group by a order by b desc having sum(c) < 7.0
    res = Data.query(df, [(col=1, group=true), (col=3, aggregate=sum, sort=true, sortasc=false, having=x->x < 7.0)], sink)
end
