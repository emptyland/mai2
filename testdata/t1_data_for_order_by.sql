
create table t1 {
    id int primary key auto_increment,
    name varchar(255) not null,
    nick char(9) not null,
    factor float not null,
    `order` BIGINT not null
};

insert into t1(name, nick, factor, `order`) values
('aaa', '001', -0.1, 10),
('bbb', '002', 0.1, 20),
('aaa', '003', 0.2, 30),
('ccc', '003', 0.4, 40),
('xxx', '010', 0.001, 50)
;