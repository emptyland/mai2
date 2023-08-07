create table t1 {
    id int primary key,
    name varchar(255) not null,
    ver varchar(255) default concat('hello', ',world'),
    nn int default 0
};

insert into t1(id, name, ver, nn) values
(1111, "hello", "1.1", 0),
(2222, "aaa", "1.1", 1),
(3333, "ccc", "1.1", 3),
(4444, "demo", "1.1", 5),
(5555, "doom", "1.1", 6),
(6666, "x-ray", "1.1", 9),
(7777, "ddt", "1.1", 11),
(8888, "dtt", "1.1", 13)
;