
drop table compare1;
drop table compare2;
create table compare1(id1 int, id2 varchar(20), name varchar(20), salary int, cashdate datetime(3), amount numeric(18, 4), amount2 decimal(18,6));

create table compare2(id1 int, id2 varchar(20), name varchar(20), salary int, cashdate datetime(3), amount numeric(18, 4), amount2 decimal(18,6));

insert into compare1 values (1, 'a', 'match', 20000, '2022-02-02 00:00:00', 13.0001, 14.999999);
insert into compare1 values (1, 'b', 'axe', 20000, '2022-02-02 00:00:00.223', 13.9999, 14.0001);
insert into compare1 values (1, 'c', 'max-p', 20000, '2022-02-02 00:00:00.999', 0.9999, 14.000001);
insert into compare1 values (1, 'd', 'tax', 20000, '2022-02-02 00:00:00.229', 0.0001, 14.9999);
insert into compare1 values (1, 'e', 'trex', 20000, '2022-02-02 00:00:00.339', 13.0001, 0.999999);
insert into compare1 values (2, 'a', 'crex', 20000, '2022-02-02 00:00:00.123', 13.0001, 0.000001);
insert into compare1 values (2, 'b', 'drex', 20000, '2022-02-02 00:00:00.321', 13.0001, 14.999999);
insert into compare1 values (2, 'c', '', 20000, '2022-02-02 00:00:00.001', 13.0001, 14.999999);
insert into compare1 values (2, 'd', Null, 20000, Null, 13.0001, 14.999999);
insert into compare1 values (2, 'e', 'mint', 20000, Null, 13.0001, 14.999999);

insert into compare2 values (1, 'a', 'match', 20000, '2022-02-02 00:00:00.001', 13.0001, 14.999999);
insert into compare2 values (1, 'b', 'axe', 20000, '2022-02-02 00:00:00.221', 13.9999, 14.0001);
insert into compare2 values (1, 'c', 'max😐p', 20000, '2022-02-02 00:00:00.099', 0.9999, 14.000001);
insert into compare2 values (1, 'd', 'tax', 20000, '2022-02-02 00:00:00.229', 0.0001, 14.9999);
insert into compare2 values (1, 'e', 'trex', 20000, '2022-02-02 00:00:00.349', 13.0001, 1.0);
insert into compare2 values (2, 'a', 'crex', 20000, '2022-02-02 00:00:00.123', 13.0002, 0.000001);
insert into compare2 values (2, 'b', 'drex', 20000, '2022-02-02 00:00:00.321', 13.0001, 15.0);
insert into compare2 values (2, 'c', Null, 20000, '2022-02-02 00:00:00.001', 13.0001, 14.999999);
insert into compare2 values (2, 'd', '', 20000, '2022-02-02 00:00:00.001', 13.0001, 14.999999);
insert into compare2 values (2, 'e', 'mint', 20000, Null, 13.0001, 14.999999);

select count(*) from compare1;
select count(*) from compare2;
