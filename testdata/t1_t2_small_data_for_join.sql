
create table t1 {
    id bigint primary key,
    dd int
};

insert into table t1(id, dd) values
    (1, 100),
    (2, 200),
    (3, 300)
;

create table t2 {
    id bigint primary key,
    name varchar(255)
};

insert into table t2(id, name) values
    (0, "none"),
    (1, "hello"),
    (2, "lol"),
    (3, "world")
;

