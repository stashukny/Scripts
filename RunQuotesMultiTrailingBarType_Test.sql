ALTER PROCEDURE RunQuotesMultiTrailingBarType @TrendPercentage decimal(18,2) = 0.6,
													@RiskPercentage decimal(18,2) = 0.5,
													@GainsPercentage decimal(18,2) = 0.5,
													@IsHighLowInclude BIT = 1,
													@IsTrailingLosses BIT = 1,
													@IsTrailingGains BIT = 1,
													@BarType varchar(10) = '2min',
													@Debug BIT = 1
			 AS
BEGIN


	if object_id('tempdb..#Trend') is not null
		drop table #Trend

	select *, 			
			[Open] - [Close] as Delta,
			CASE WHEN ([Open] - [Close]) > 0 THEN 'Down' ELSE 'Up' END as Direction,
			ABS([Open] - [Close]) / [Open] * 100 as Percentage,
			ROW_NUMBER() OVER(ORDER BY Date, Time) as Swing,
			'OpenClose' as Pattern
	into #Trend
	from Quotes
	where BarType = @BarType
		and ABS([Open] - [Close]) / [Open] * 100 > @TrendPercentage		
	order by Percentage desc


	if (@IsHighLowInclude = 1)
	begin
		insert into #Trend
		select q.*, 
				q.[Open] - q.[Close] as Delta,
				CASE WHEN (q.[Open] - q.[Close]) > 0 THEN 'Down' ELSE 'Up' END as Direction,
				ABS(q.[High] - q.[Low]) / q.[High] * 100 as Percentage,
				ROW_NUMBER() OVER(ORDER BY q.Date, q.Time) as Swing,
				'HighLow' as Pattern
		from Quotes	q
			left join #Trend t
			on q.Date = t.Date
			and q.Time = t.Time
			and q.BarType = t.BarType
		where q.BarType = @BarType
			and ABS(q.[High] - q.[Low]) / q.[High] * 100 > @TrendPercentage
			and t.Date is null
		order by Percentage desc
	end

	if (@Debug = 1)
		select *
		from #Trend

	if object_id('tempdb..#SinceSwing') is not null
		drop table #SinceSwing

	select q.[Date], q.[Time],q.[Open], q.[Close], q.[Open] - q.[Close] as Delta,
			p.Direction,
			p.Percentage,
			Pattern,
			p.Swing,
			ROW_NUMBER() OVER(PARTITION BY q.Date, p.Swing, Pattern ORDER BY q.Time) as Sequence
	into #SinceSwing
	from Quotes q
		join #Trend p
		on p.Date = q.Date
		and DATEDIFF(MINUTE, p.[Time], q.[Time]) > 0
		and q.BarType = p.BarType
	where q.BarType = @BarType


	if object_id('tempdb..#Entries') is not null
		drop table #Entries

	select [Date], [Time], [Open], Swing, Pattern
	into #Entries
	from #SinceSwing 
	where Sequence = 1

	if (@Debug = 1)
		select *
		from #Entries

	if object_id('tempdb..#Results') is not null
		drop table #Results

	select t1.Date, t1.Time, Sequence, e.Swing, t1.Pattern, Direction, CONVERT(decimal(18,2), Percentage) as Percentage,		
			sum(t1.[Open]) as [Open], 
			sum(t1.[Close]) as [Close], 
			sum(e.[Open]) as Entry,
			sum(case when t1.Direction = 'Up' then t1.[Close] - e.[Open] else e.[Open] - t1.[Close] end) as Exposure,
			convert(decimal(18,2), sum(case when t1.Direction = 'Up' then t1.[Close] - e.[Open] else e.[Open] - t1.[Close] end / e.[Open] * 100)) as ExposurePercentage
	into #Results
	from #SinceSwing t1 
		join #Entries e
		on T1.Date = e.Date
		and t1.Swing = e.Swing
		and t1.Pattern = e.Pattern
	group by t1.Date, t1.Time, e.Swing, t1.Pattern, Sequence, Direction, Percentage
	order by t1.Date, t1.Time, e.Swing, Pattern, Sequence


	if object_id('tempdb..#Shift') is not null
		drop table #Shift


			select r1.*,
				
					case when r1.Direction = 'Up' then
						case when @IsTrailingLosses = 1 then 

							case when r2.[Close] > r2.[Open]  then 
								r2.[Close] - (r2.[Close] / 100.0 * @RiskPercentage) 
							else r2.[Open] - (r2.[Open] / 100.0 * @RiskPercentage) end

						else r2.[Entry] - (r2.[Entry] / 100.0 * @RiskPercentage) end

					else 
						case when @IsTrailingLosses = 1 then  
							case when r2.[Close] < r2.[Open] then 
								r2.[Close] + (r2.[Close] / 100.0 * @RiskPercentage) 
							else r2.[Open] + (r2.[Open] / 100.0 * @RiskPercentage)  end 
						else r2.[Entry] + (r2.[Entry] / 100.0 * @RiskPercentage) end end as CutLossesLevel,

					case when r1.Direction = 'Up' then
						case when @IsTrailingGains = 1 then 
							case when r2.[Close] > r2.[Open]  then 
								r2.[Close] + (r2.[Close] / 100.0 * @GainsPercentage) 
							else r2.[Open] + (r2.[Open] / 100.0 * @GainsPercentage) end
						else r2.[Entry] + (r2.[Entry] / 100.0 * @GainsPercentage) end

					else 
						case when @IsTrailingGains = 1 then  
							case when r2.[Close] < r2.[Open] then 
								r2.[Close] - (r2.[Close] / 100.0 * @GainsPercentage) 
							else r2.[Open] - (r2.[Open] / 100.0 * @GainsPercentage)  end 
						else r2.[Entry] - (r2.[Entry] / 100.0 * @GainsPercentage) end end as CollectGainsLevel,
				convert(decimal(18,2), NULL) as CutLosses,
				convert(decimal(18,2), NULL) as CollectGains


	into #Shift
	from #Results r1
		join #Results r2
		on r1.Date = r2.Date
		and r1.Pattern = r2.Pattern
		and r1.Swing = r2.Swing
		and r1.Sequence = r2.Sequence + 1

	
	update #Shift
	set CutLosses = case when Direction = 'Up' then 
						case when [Close] <= CutLossesLevel then [Close] else NULL end
					else case when [Close] >= CutLossesLevel then [Close] else NULL end end,
		CollectGains = case when Direction = 'Up' then 
						case when [Close] >= CollectGainsLevel then [Close] else NULL end
					else case when [Close] <= CollectGainsLevel then [Close] else NULL end end
			

	--------------------------------------
	-- Take first Loss or Gain
	--------------------------------------
	if object_id('tempdb..#RealizedResults') is not null
		drop table #RealizedResults

	select *, ROW_NUMBER() OVER(PARTITION BY Date, Swing, Pattern ORDER BY Date, Swing, Pattern, Time) as RealizedSequence
	into #RealizedResults
	from #Shift
	where [CutLosses] is not null
		or [CollectGains] is not null

	insert into #RealizedResults
	select r.*, ROW_NUMBER() OVER(PARTITION BY r.Date, r.Swing, r.Pattern ORDER BY r.Date, r.Swing, r.Pattern, r.Time) as RealizedSequence
	from #Shift r
		left join #RealizedResults rr
		on r.Date = rr.Date
		and r.Swing = rr.Swing
	where rr.Date is null
		and r.Time = (select max(Time) from #Results a where Date = r.Date and Swing = r.Swing and Pattern = r.Pattern)

	delete from #RealizedResults
	where RealizedSequence <> 1

	select Date, 
		count(distinct Swing) as Swings,
		sum(isnull(ExposurePercentage, 0)) as OverallTotal
	from #RealizedResults
	group by Date


	select (select count(distinct Date) from Quotes where BarType = @BarType) as DatesAnalyzed,
			count(distinct date) as DatesTraded, 
			count(1) * 2 as Trades, 
			sum(isnull(ExposurePercentage, 0)) as OverallTotal
	from #RealizedResults


END
GO

exec RunQuotesMultiTrailingBarType @TrendPercentage = 0.6,
													@RiskPercentage = 0.5,
													@GainsPercentage = 0.5,
													@IsHighLowInclude = 1,
													@IsTrailingLosses = 1,
													@IsTrailingGains = 1,
													@BarType = '2min',
													@Debug = 0

exec RunQuotesMultiTrailingBarType @TrendPercentage = 1.15,
													@RiskPercentage = 1.15,
													@GainsPercentage = 1.15,
													@IsHighLowInclude = 0,
													@IsTrailingLosses = 0,
													@IsTrailingGains = 0,
													@BarType = '5min',
													@Debug = 0

exec RunQuotesMultiTrailingBarType @TrendPercentage = 1.3,
													@RiskPercentage = 1.3,
													@GainsPercentage = 1.3,
													@IsHighLowInclude = 0,
													@IsTrailingLosses = 0,
													@IsTrailingGains = 0,
													@BarType = '10min',
													@Debug = 0