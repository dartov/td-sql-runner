drop table {{ mytablename }};

create table {{ mytablename }} as (select * from src_table) with data;

select * from {{ mytablename }};