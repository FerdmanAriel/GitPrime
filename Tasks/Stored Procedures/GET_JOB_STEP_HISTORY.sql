--EZMANAGE_
create procedure [Tasks].[GET_JOB_STEP_HISTORY] (
			@Job_ID nvarchar(500),
			@Start datetime=null,
			@End datetime=null,
			@sort nvarchar(100)='run_datetime,instance_duration',
			@sort_direction char(4)='DESC', -- ASC DESC
			@page_size int = 100, 
			@page_number int = 1,
			@Filter_Run_Status int = null
			--@Filter_Step_Name nvarchar(1000)= null,
			--@Filter_Global_String nvarchar(4000)= null
	) as
	begin
		if (@Start is null)
			set @Start =  dateadd(day,-14,getdate())
		if (@End is null)
			set @End = getdate()

		declare @sqlFrom nvarchar(max) 

			declare @sql nvarchar(max) 
			set @sql = N' 
			
 create table #baseWithGroupInstance
 (	
		[run_datetime] datetime not null,
				  instance_id int,
					job_id nvarchar(500),
					step_id int,
					step_name nvarchar(500),
					sql_message_id int,
					sql_severity int,
					message nvarchar(4000),
					run_status int ,
					run_date nvarchar(50),
					run_time nvarchar(50),
					run_duration int,
					retries_attempted int,
					instanceGroup int null,
					steps int null)

			-- inserting all records after filtering the job id by dates
			insert into #baseWithGroupInstance (run_datetime,instance_id,job_id,step_id,step_name,sql_message_id,sql_severity,message,run_status,run_date,run_time,
			run_duration,retries_attempted)
				select 
				msdb.dbo.agent_datetime(history.run_date,history.run_time) [run_datetime],
				  instance_id,
					job_id,
					step_id,
					step_name,
					sql_message_id,
					sql_severity,
					message,
					run_status,
					run_date,
					run_time,
					 (run_duration/10000 * 60 * 60) + -- hours as seconds
						   (run_duration/100%100 * 60) + --minutes as seconds
						   (run_duration%100 ) run_duration,
					retries_attempted
				from  msdb..sysjobhistory history 
				where job_id = @job_id and msdb.dbo.agent_datetime(history.run_date,history.run_time) between @Start and @End

				
				DECLARE @instance_id int
				declare @step_id int
				declare @instanceGroup int 
				set @instanceGroup  = 1
				declare @steps int 
				set @steps = 0
				
				DECLARE db_cursor CURSOR FOR 
				SELECT instance_id,step_id
				FROM #baseWithGroupInstance
				order by instance_id

				OPEN db_cursor  
				FETCH NEXT FROM db_cursor INTO @instance_id,@step_id

				WHILE @@FETCH_STATUS = 0  
				BEGIN  
						update #baseWithGroupInstance
							set instanceGroup = @instanceGroup
						where instance_id = @instance_id

						 if (@step_id = 0)
						 begin
							set @instanceGroup = @instanceGroup+1

							update #baseWithGroupInstance
							set steps = @steps
							where instance_id = @instance_id

							set @steps = -1
						end

						set @steps = @steps + 1

					  FETCH NEXT FROM db_cursor INTO @instance_id,@step_id 
				END 

				CLOSE db_cursor  
				DEALLOCATE db_cursor

				-- now filtering the groups that their step_id = 0 has the conditions (instance filter)
				if (@Filter_Run_Status is not null and @Filter_Run_Status between 0 and 7)
					delete #baseWithGroupInstance where instanceGroup in (select instanceGroup from #baseWithGroupInstance where step_id = 0 and run_status != @Filter_Run_Status)

				  select a.*, b.run_duration as [instance_duration] 
				  into #withInstanceDuration
				  from #baseWithGroupInstance a
					inner join #baseWithGroupInstance b on a.instanceGroup = b.instanceGroup  
					and b.step_id = 0'

			declare @versionOld bit 
			set @versionOld = 0
			if (@@Version like '%2005%' or @@Version like '%2008%')
					 set @versionOld = 1
	
			--set @versionOld = 1
			if (@versionOld = 0)
				set @sql = @sql + '
				-- now we have inside the instanceGroups. We bring back the unique instance IDs 
				;WITH ResultCTE AS(
						        select instanceGroup,instance_duration,max(run_datetime) as run_datetime 
								from #withInstanceDuration
							   group by instanceGroup,instance_duration
						    ),
							TotalRows AS(SELECT Count(1) AS MaxRows FROM ResultCTE)	
								SELECT * into #t3 FROM TotalRows, ResultCTE order by '+@sort + '  ' + @sort_direction +' '+ 
								' OFFSET ' 	+cast (@page_size*(@page_number-1) as nvarchar(100)) + ' ROWS FETCH NEXT '+cast(@page_size as nvarchar(100)) +'  ROWS ONLY; 

				;WITH ResultCTE AS(
						       select * from #withInstanceDuration a
							   where instanceGroup in (select instanceGroup from #t3)
						    ),
							TotalRows AS(SELECT Count(1) AS MaxRows FROM ResultCTE)	
								SELECT * into #t4 FROM TotalRows, ResultCTE '+
								 ' order by '+@sort+' ' + @sort_direction +
								
			+ ' select * from #t4  order by '+@sort+' ' + @sort_direction + '
				select top 1 MaxRows from #t3;'
			else
				set @sql = @sql + '
				
				select instanceGroup,instance_duration,max(run_datetime) as run_datetime 
				into #t3
				from #withInstanceDuration
				group by instanceGroup,instance_duration

				select * 
				into #t4 
				from #withInstanceDuration a
				where instanceGroup in (select instanceGroup from #t3)
								
				select * from #t4  order by '+@sort+' ' + @sort_direction + '
				select count(*) as MaxRows from #t3;'

		print @sql
		exec sp_executesql @sql,N'@Job_ID nvarchar(500),@Start datetime, @End datetime, 
			@Filter_Run_Status int
			',
			@Job_ID =@Job_ID,
			@Start=@Start ,
			@End=@End ,
			@Filter_Run_Status =@Filter_Run_Status 
	
	--exec EZManagePro.[Tasks].[GET_JOB_STEP_HISTORY]
	--		@Job_ID = '1365dc95-665f-45a9-bfc8-57e1f630e7ed',
	--		--@Start datetime=null,
	--		--@End datetime=null,
	--		@sort ='run_datetime', -- run_duration, step_name, step_id, sql_message_id, sql_severity, run_status, run_time
	--		@sort_direction ='DESC', -- ASC DESC
	--		@page_size = 25,
	--		@page_number = 1,
	--		--@Filter_Run_Status int = null,
	--		--@Filter_Step_Name nvarchar(1000)= null,
	--		@Filter_Global_String = 'aaa'

	--	exec [EZManagePro].[Tasks].[GET_JOB_STEP_HISTORY] @Job_ID=N'e50ca8e4-fb8c-4668-ac3a-97c0de387663',@Start=NULL,
	--@End=NULL,@sort=N'run_datetime',
	--@sort_direction=N'DESC',@page_size=10,@page_number=1,@Filter_Run_Status=NULL

	end