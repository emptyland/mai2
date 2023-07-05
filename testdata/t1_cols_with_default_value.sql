

create table t1 {
    id int primary key auto_increment,
    name varchar(255) not null,
    ver varchar(255) default version(),
    nn int default 0
}