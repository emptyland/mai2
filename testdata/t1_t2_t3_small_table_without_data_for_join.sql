create table t1 {
    id bigint primary key,
    dd int,
    dc int,
    name varchar
(
    255
)
    index index_dd
(
    dd
)
    };

create table t2 {
    id bigint primary key,
    df int,
    de int,
    name varchar
(
    255
)
    index index_df
(
    df
)
    };

create table t3 {
    part1 bigint,
    part2 int,
    part3 varchar
(
    255
),
    dt int
    constraint primary key
(
    part1,
    part2,
    part3
)
    };