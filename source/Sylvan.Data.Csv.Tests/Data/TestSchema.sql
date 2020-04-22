
create table Simple1 (
Id int not null,
Name nvarchar(32) not null
)

create type dbo.Simple1Type as table (
	Id int not null,
	Name nvarchar(32) not null
)
go

create proc InsertSimple1
@Data dbo.Simple1Type readonly
as
insert into Simple1
select * from @Data


go

create table Simple2 (
	Id int not null,
	Name nvarchar(32) not null,
	Code char(2) not null
)

create type dbo.Simple2Type as table (
	Id int not null,
	Name nvarchar(32) not null,
	Code char(2) not null
)
go

create proc InsertSimple2
@Data dbo.Simple2Type readonly
as
insert into Simple2
select * from @Data