--EZMANAGE_
create procedure [Tasks].[GET_DBCOMPARE_DATA] (@database_name nvarchar(256) ='')
as 
begin
declare @sql nvarchar(max)

set @sql = 'use ['+@database_name + ']
-- Table [0] tables without structure

if (object_id(''#tempTableList'') is not null) drop table #tempTableList

select top 30 ''Tables'' as [CompareType],
	schema_name(T.schema_id) [Schema_Name_No_P] ,t.Name [Table_Name_No_P] ,
	''['' + schema_name(T.schema_id) + '']'' [Schema_Name] , ''['' + t.Name + '']'' [Table_Name] , 
	''['' + schema_name(T.schema_id) + ''].['' + t.Name + '']'' [Full_Table_Name] ,t.modify_date Last_Modified
into #tempTableList
from sys.tables t

alter table #tempTableList add [Definition] nvarchar(max)

DECLARE @schemaName VARCHAR(50) -- database name 
DECLARE @tableName VARCHAR(256) -- path for backup files 

DECLARE db_cursor CURSOR FOR 
SELECT [Schema_Name_No_P],[Table_Name_No_P] 
FROM #tempTableList

OPEN db_cursor  
FETCH NEXT FROM db_cursor INTO @schemaName,@tableName  

WHILE @@FETCH_STATUS = 0  
BEGIN  
	  exec [EZManagePro].[Tasks].[sp_CloneTable] N'''+@database_name + ''',@schemaName,@tableName
      FETCH NEXT FROM db_cursor INTO @schemaName,@tableName 
END 

CLOSE db_cursor  
DEALLOCATE db_cursor

select * from #tempTableList

'

print @sql
exec sp_executesql @sql

set @sql = N'
-- [1] - table columns
select ''TableColumns'' as [CompareType],''['' + schema_name(t.schema_id) + '']'' Schema_Name ,''['' + t.Name +'']'' [Table_Name],  c.name [Column_Name], 
c.column_id [Column_Order],
t2.name [Type],c.max_length Max_Length,c.precision Precision,c.scale Scale,c.collation_name [Collation_Name],
c.is_nullable [Is_Nullable],c.is_identity [Is_Identity]
from 
	sys.columns c inner join sys.tables t  on c.object_id = t.object_id
	left outer join sys.types t2 on c.system_type_id = t2.system_type_id and c.system_type_id = t2.user_type_id
	--where t.Name = ''t1''
order by 1,c.column_id

-- [2] indexes + columns
SELECT	''TableIndexes'' as [CompareType],	
''[''+ss.[name]   +'']'' Schema_Name ,''[''+ so.name +'']'' [Table_Name],
			si.[name]   ''Index_Name'',
			si.[type_desc] ''Index_Type'',
			si.[is_unique] ''Is_Unique'',
			si.[is_primary_key] ''Is_Primary_Key'',
			si.[is_unique_constraint] ''Is_Unique_Constraint'',
			STUFF((
				SELECT '', ['' + sc.NAME + '']''  AS "text()"
				FROM syscolumns AS sc
				INNER JOIN sys.index_columns AS ic ON ic.object_id = sc.id
					AND ic.column_id = sc.colid
				WHERE sc.id = so.object_id
					AND ic.index_id = si.index_id
					AND ic.is_included_column = 0
				ORDER BY key_ordinal
				FOR XML PATH('''')
            ), 1, 2, '''') AS ''Key_Columns''
			,STUFF((
					SELECT '', ['' + sc.NAME + '']''  AS  "text()"
					FROM syscolumns AS sc
					INNER JOIN sys.index_columns AS ic ON ic.object_id = sc.id
						AND ic.column_id = sc.colid
					WHERE sc.id = so.object_id
						AND ic.index_id = si.index_id
						AND ic.is_included_column = 1
					FOR XML PATH('''')
					), 1, 2, '''') AS ''Included_Columns''
FROM		sys.objects	AS so
			INNER JOIN
			sys.indexes	AS si
			ON so.[object_id]	=	si.[object_id]
			INNER JOIN
			sys.schemas	AS ss
			ON	ss.[schema_id]	=	so.[schema_id]
			LEFT JOIN
			(
			SELECT	ss.[name]		 		as [schema_name],
					so.[name]		 		as [Object_Name], 
					si.[name]		 		as [Index_Name],
					SUM(sau.[total_pages])	as [Total_Pages], 
					SUM(sau.[used_pages])	as [Used_Pages], 
					SUM(sau.[data_pages])	as [Data_Pages]
			FROM	sys.objects AS so
					INNER JOIN
					sys.indexes AS si
					ON so.[object_id]	=	si.[object_id]
					INNER JOIN
					sys.schemas AS ss
					ON	so.[schema_id]	=	ss.[schema_id]
					INNER JOIN 
					sys.partitions AS sp
					ON		si.[object_id]	=	sp.[object_id]
							AND 
							si.[index_id]	=	sp.[index_id]
					INNER JOIN 
					sys.allocation_units sau
					ON		sp.[partition_id]	=	sau.[container_id]
			WHERE	si.[index_id] >= 1
					AND
					so.[is_ms_shipped]	=	0
					
			GROUP BY ss.[name]  , so.[name]  ,si.[name]	  
			) AS [is]
			ON	ss.[name]	 	=	[is].[schema_name]  
				AND
				so.[name]	 	=	[is].[Object_Name]  
				AND
				si.[name]	 	=	[is].[Index_Name]  
WHERE		so.[type_desc]  IN (N''USER_TABLE'')
			AND
			si.index_id > 0
			AND
			so.[is_ms_shipped]	=	0
			and 
			si.[type_desc] not in (''XML'')
			AND 
			so.[name] not like ''_MPStats_Sys_%''
			--and
			--so.name =''Server_Alerts_History''

    
	-- [4] table Foreign key
    select ''TableForeignKeys'' as [CompareType],
	''[''+schema_name(fk_tab.schema_id) + '']'' Schema_Name, ''['' + fk_tab.[name] +'']'' as Table_Name,
        fk.name as FK_Constraint_Name,
     ''[''+   schema_name(pk_tab.schema_id) + ''].['' + pk_tab.name+'']'' FK_Columns
    from sys.foreign_keys fk
        inner join sys.tables fk_tab
            on fk_tab.object_id = fk.parent_object_id
        inner join sys.tables pk_tab
            on pk_tab.object_id = fk.referenced_object_id
	--where fk_tab.[name] = ''Sessions''
			'


exec sp_executesql @sql	, N'@database_name nvarchar(512)',@database_name=@database_name

set @sql = ' use ['+@database_name + ']
	-- [4] table Check constraints
    select ''TableConstraints'' as [CompareType],
	''[''+schema_name(t.schema_id) + '']'' Schema_Name ,''['' + t.[name] +'']'' as Table_Name,
        con.[name] as Constraint_Name,
        con.[definition] [Constraint_Definition]
    from sys.check_constraints con
        left outer join sys.objects t
            on con.parent_object_id = t.object_id
        left outer join sys.all_columns col
            on con.parent_column_id = col.column_id
            and con.parent_object_id = col.object_id
    union all
	-- [6] table Default Constraints
    select 	''TableConstraints'' as [CompareType],''[''+schema_name(t.schema_id) + '']'' Schema_Name ,''['' + t.[name] +'']'' as Table_Name,
        con.[Name] [Column_Name],
        col.[name] + '' = '' + con.[definition] as [Constraint_Definition]
    from sys.default_constraints con
        left outer join sys.objects t
            on con.parent_object_id = t.object_id
        left outer join sys.all_columns col
            on con.parent_column_id = col.column_id
            and con.parent_object_id = col.object_id


-- [7] - scriptable objects + their original script (storage procedure, functions, triggers, views)
select ''Functions'' [CompareType],''['' + schema_name(o.schema_id) + '']'' Schema_Name ,''['' + o.Name + '']'' Name, case when m.definition is null then ''--encrypted'' else m.definition end  [Definition],o.modify_date Last_Modified
from sys.sql_modules m
inner join sys.objects o on m.object_id = o.object_id
where o.type in (''FN'',''IF'',''TF'')

select  ''StoredProcedures''  [CompareType],''['' + schema_name(o.schema_id) + '']'' Schema_Name ,''['' + o.Name + '']'' Name, 
 case when m.definition is null then ''--encrypted'' else m.definition end [Definition],o.modify_date Last_Modified
from sys.sql_modules m
inner join sys.objects o on m.object_id = o.object_id
where o.type in (''P'') --and o.name =''Get_Alert_Values''

select  ''Triggers'' [CompareType],''['' + schema_name(o.schema_id) + '']'' Schema_Name ,''['' + o.Name + '']'' Name, 
 case when m.definition is null then ''--encrypted'' else m.definition end [Definition],o.modify_date Last_Modified
from sys.sql_modules m
inner join sys.objects o on m.object_id = o.object_id
where o.type in (''TR'')

select ''Views''  [CompareType],''['' + schema_name(o.schema_id) + '']'' Schema_Name ,''['' + o.Name + '']'' Name, 
 case when m.definition is null then ''--encrypted'' else m.definition end [Definition],o.modify_date Last_Modified
from sys.sql_modules m
inner join sys.objects o on m.object_id = o.object_id
where o.type in (''V'')

-- [8] - users
select ''Users'' as [CompareType],Name User_Name,type_desc User_Type,default_database_name Default_Database
from 
master.sys.sql_logins'


exec sp_executesql @sql

end