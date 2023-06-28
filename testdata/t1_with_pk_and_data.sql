create table t1 {
    id int primary key auto_increment,
    name varchar(255),
    a char(9),
    b int index idx_b(b)
};

insert into table t1(name, a, b)
values
    ("a", "xxx", 200)
  , ("aa", "xxx", 300)
  , ("aa", "xxx", 301)
  , ("aaa", "xxx", 400)
  , ("aaaa", "xxx", 500)
  , ("aaaaa", "xxx", 600)
  , ("aaaaa", "xxx", 601)
  , ("aaaaa", "xxx", 602)
  , ("aaaaaa", "xxx", 700)
;