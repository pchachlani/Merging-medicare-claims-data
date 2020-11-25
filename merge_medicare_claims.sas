/*Merging the Medicare claims data without sorting*/
/*getting the BETOS CODES list*/

Proc sql;
	select quote(betos) into: betos_cd_list separated by "," from cmn.betos_cd;
quit;

/*merging the episode level dataset with the chronic diagnosis codes and the medicare claims datasets */

data out.master_episodes;
	length link_id 8. episode_id 8. epi_group_name $200. grouping_start_dt 8. betos_Cd $3. dgns_Cd $8.;
	format grouping_start_dt date9.;
	if _n_=1 then do;
		if 0 then set rf.chronic_episodes cm.dgn_codes;
		declare hash epi(dataset:"rf.chronic_episodes", multidata:"y");
		epi.definekey("link_id");
		epi.definedata("epi_group_id", "epi_group_name", "episode_id", "grouping_start_dt");
		epi.definedone();

		declare hash ccw(dataset:"cm.dgn_codes");
		ccw.definekey("dgns_cd");
		ccw.definedone();

	end;
	call missing (of _all_);
	set 
		p4p.ip_stays (in=in_ip_stays)
		p4p.sn (in=in_sn)
		p4p.hhl (in=in_hh)
		p4p.pb (in=in_pb)
		p4p.opl (in=opl)
		end=last;
	length flag_win 3.; flag_win=0;
	if ccw.find()=0 and epi.find()=0 then do;
		flag_win=1;	
	end;
run;


/*calculate summary stats*/

proc sql;
	create table temp.epi_bene_count as
	select
		epi_group_name,
		count(distinct case when flag_win=1 then episode_id else . end) as epi_count,
		count(distinct case when flag_win=1 then link_id else . end) as bene_count
	from outer union.master_episodes
	group by epi_group_name;
quit;




