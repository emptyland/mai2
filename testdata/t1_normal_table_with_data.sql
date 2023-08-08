create table t1 {
    id int primary key,
    name varchar(255) not null,
    ver varchar(255) default concat('hello', ',world'),
    nn int default 0
};

insert into t1(id, name, ver, nn) values
(1111, "hello", "1.1", 0),
(2222, "aaa", "1.2", 1),
(3333, "ccc", "1.3", 3),
(4444, "demo", "2.1", 5),
(5555, "doom", "2.2", 6),
(6666, "x-ray", "3.1", 9),
(7777, "ddt", "4.1", 11),
(8888, "dtt", "4.2", 13)
;