create table Feature (
	FeatureId int not null,
	FeatureName varchar(128) not null,
	FeatureClass varchar(50) not null,
	StateAlpha varchar(3) null,
	StateNumeric char(2) null,
	CountyName varchar(100),
	CountyNumeric char(3) null,
	PrimaryLatDMS char(7) null,
	PrimaryLonDMS char(8) null,
	PrimaryLatDEC float null,
	PrimaryLonDEC float null,
	SourceLatDMS char(7) null,
	SourceLonDMS char(8) null,
	SourceLatDEC float null,
	SourceLonDEC float null,
	ElevationMeters int null,
	ElevationFeet int null,
	MapName varchar(100) null,
	DateCreated date null,
	DateEdited date null
)

create type dbo.FeatureType as table (
	FeatureId int not null,
	FeatureName varchar(128) not null,
	FeatureClass varchar(50) not null,
	StateAlpha varchar(3) null,
	StateNumeric char(2) null,
	CountyName varchar(100),
	CountyNumeric char(3) null,
	PrimaryLatDMS char(7) null,
	PrimaryLonDMS char(8) null,
	PrimaryLatDEC float null,
	PrimaryLonDEC float null,
	SourceLatDMS char(7) null,
	SourceLonDMS char(8) null,
	SourceLatDEC float null,
	SourceLonDEC float null,
	ElevationMeters int null,
	ElevationFeet int null,
	MapName varchar(100) null,
	DateCreated date null,
	DateEdited date null
)
go

create proc InsertFeatures
@Data dbo.FeatureType readonly
as
insert into Feature
select * from @Data