create table t3 {
    id bigint primary key auto_increment,
    dd int,
    name varchar(255)
    index index_dd(dd)
};

insert into table t3(dd, name)
values
    (100, "Js")
  , (101, "Jc")
  , (102, "Jk")
  , (102, "Ol")
;

create table t4 {
    id bigint primary key auto_increment,
    df int,
    name varchar(255)
    index index_df(df)
};

insert into table t4(df, name)
values
    (101, "Js")
  , (101, "Jc")
  , (102, "Jk");