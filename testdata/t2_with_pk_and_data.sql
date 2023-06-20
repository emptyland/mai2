create table t2 {
    id int primary key auto_increment,
    name varchar
(
    255
),
    a char
(
    9
),
    b int
    index idx_b
(
    b
)
    };

insert into table t2(name, a, b)
values
    ("b", "xxx", 200), ("bb", "xxx", 300), ("bb", "xxx", 301), ("bbb", "xxx", 400), ("bbbb", "xxx", 500), ("bbbbb", "xxx", 600), ("bbbbb", "xxx", 601), ("bbbbb", "xxx", 602), ("bbbbbb", "xxx", 700);