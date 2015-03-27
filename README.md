This is a single-user DBMS program me and my batchmate wrote for my Advanced Databases course last semester.

It's simulates a database by storing data in terms of pages in main memory. Whenever a user requests data,
appropriate pages are searched and maintained in LRU. Caching of pages is simulated.

Every database has it's own structure which contains the metadata of the database. Queries have their own
structure and are treated as objects.

Merging of data is also carried through a 2 phase merge sort which takes 2 pages at a time and merges them.

Queries supported: select, where, orderby and join.

Usage:
	g++ path/main.cpp
	./a.out <config file>  ( Read the sample config file to know the syntax )
        > Ask queries

Sample Usage:
	./a.out config.txt
	select * from table1
	select * from table1 where table.e_id<167
        select e_num from table1 where table1.e_id<167 and table1.e_order=462
	select s_id,s_order,s_num from table2 where table2.s_num<230 and table2.s_num>205 or table2.s_idLIKE48 orderby table2.s_num(DESC)

Mihir Wadwekar
Aayush Saxena
