select c.c_name, o.o_orderkey, o.o_orderdate
from customer as c
inner join orders as o on c.c_custkey = o.o_custkey
where c.c_custkey < 3
order by c.c_name, o.o_orderkey