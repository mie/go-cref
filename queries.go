package main

import (
	"fmt"
	"strings"
)

func GetCaseQuery (caseid *string) string {
	return `select c.id, a.name, m.name from cases c
	inner join analyses a on a.sfid = c.analysissfid
	inner join models m on m.sfid = a.modelsfid
	where c.id = ` + *caseid
}

func GetFormulasQuery (caseid *string, attrs *[]string, template *string, model *string) string {
	return fmt.Sprintf(`select o.sfname, a.afattribute, a.afreference, a.settingsorigin from templates t 
	inner join cases c on 1=1 and c.id = %s
	inner join objects o on o.templatesfid = t.sfid and o.createcaseid <= c.id and (o.deletecaseid is null or o.deletecaseid > c.id)
	inner join models m on m.sfid = o.modelsfid
	inner join attrsettings a on a.afelement = o.sfid and a.isdeleted = 0 and a.afattribute in ('%s')
	where t.name = '%s' and m.name = '%s'`, *caseid, strings.Join(*attrs, "','"), *template, *model)
}

func GetTankMassQuery (caseid *string, measured *string, attribute *string, objectid *string, model *string) string {
	fmt.Println(`%s`, *measured);
	fmt.Println(`select o.sfname, '%s', cast(m.sfname as nvarchar(255)) + '|%s' s from objects o
	inner join models md on md.sfid = o.modelsfid
	inner join links l on l.destid = o.id or l.sourceid = o.id
	inner join objects ol on ol.id = l.id and ol.createcaseid <= c.id and (ol.deletecaseid is null or ol.deletecaseid > c.id)
	inner join flowsmeters fm on fm.flowid = l.flowid
	inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= c.id and (ofm.deletecaseid is null or ofm.deletecaseid > c.id)
	inner join objects m on m.id = fm.meterid
	where o.sfname = '%s' and md.name = '%s'`, *attribute, *measured, *caseid, *objectid, *model);
	return fmt.Sprintf(`select o.sfname, '%s', cast(m.sfname as nvarchar(255)) + '|%s' s from objects o
	inner join models md on md.sfid = o.modelsfid
	inner join links l on l.destid = o.id or l.sourceid = o.id
	inner join objects ol on ol.id = l.id and ol.createcaseid <= c.id and (ol.deletecaseid is null or ol.deletecaseid > c.id)
	inner join flowsmeters fm on fm.flowid = l.flowid
	inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= c.id and (ofm.deletecaseid is null or ofm.deletecaseid > c.id)
	inner join objects m on m.id = fm.meterid
	where o.sfname = '%s' and md.name = '%s'`, *attribute, *measured, *caseid, *objectid, *model)
}

func GetTheRestQuery (caseid *string, attrs *[]string, template *string, model *string) string {
	return fmt.Sprintf(`select cast(o.sfname as nvarchar(255)) + '|' + a.afattribute attr from templates t
	inner join cases c on 1=1 and c.id = %s
	inner join objects o on o.templatesfid = t.sfid and o.createcaseid <= c.id and (o.deletecaseid is null or o.deletecaseid > c.id)
	inner join models m on m.sfid = o.modelsfid
	inner join attrsettings a on a.afelement = o.sfid and a.isdeleted = 0 and
		(
			(a.afattribute in ('%s') and not a.afreference in ('Formula','Smart Sum of Transfers','Sum of Transfers','Tank Mass')) or
			not a.afattribute in ('%s')
		)
	where t.name = '%s' and m.name = '%s'`, *caseid, strings.Join(*attrs, "','"), strings.Join(*attrs, "','"), *template, *model)
}

func GetSmartSTQuery (caseid *string) string {
	return fmt.Sprintf(`
		create table #checker_table2 (meterid int, attribute nvarchar(255), metername nvarchar(255), sfname nvarchar(255))
		insert into #checker_table2
		select distinct * from (
		select distinct m.id meterid, t.attribute, t.metername, meters.sfname from links l
		inner join cases c on 1=1 and c.id = %s
		inner join objects o on o.id = l.id and o.createcaseid <= c.id and (o.deletecaseid is null or o.deletecaseid > c.id)
		inner join flowsmeters fm on fm.flowid = l.flowid
		inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= c.id and (ofm.deletecaseid is null or ofm.deletecaseid > c.id)
		inner join objects m on m.id = fm.meterid
		inner join #checker_table t on (t.nodeid = l.sourceid or t.nodeid = l.destid)
		left join objects meters on meters.sfname != t.metername and meters.id = m.id
		union all
		select distinct m.id, t.attribute, t.metername, meters.sfname from ports p
		inner join cases c on 1=1 and c.id = %s
		inner join objects o on o.id = p.id and o.createcaseid <= c.id and (o.deletecaseid is null or o.deletecaseid > c.id)
		inner join flowsmeters fm on fm.flowid = p.flowid
		inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= c.id and (ofm.deletecaseid is null or ofm.deletecaseid > c.id)
		inner join objects m on m.id = fm.meterid
		inner join #checker_table t on t.nodeid = p.unitid or t.nodeid = p.connectionobjid
		left join objects meters on meters.sfname != t.metername and meters.id = m.id) data
		select c1.metername, c1.attribute, isnull(c1.sfname, '') from #checker_table2 c1
		inner join (
			select metername, count(*) c from #checker_table2 group by metername
		) counts on counts.metername = c1.metername
		where (counts.c > 1 and c1.sfname is not null) or counts.c = 1
		order by c1.metername, c1.attribute
		drop table #checker_table; drop table #checker_table2`, *caseid, *caseid)
}

func GetSTQuery (caseid *string) string {
	return fmt.Sprintf(`
	create table #checker_table2 (meterid int, attribute nvarchar(255), metername nvarchar(255), sfname nvarchar(255))
	insert into #checker_table2
	select distinct * from (
	select distinct m.id meterid, t.attribute, t.metername, meters.sfname from links l
	inner join cases c on 1=1 and c.id = %s
	inner join objects o on o.id = l.id and o.createcaseid <= c.id and (o.deletecaseid is null or o.deletecaseid > c.id)
	inner join flowsmeters fm on fm.flowid = l.flowid
	inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= c.id and (ofm.deletecaseid is null or ofm.deletecaseid > c.id)
	inner join objects m on m.id = fm.meterid
	inner join #checker_table t on (t.nodeid = l.sourceid and t.isinput = 0) or (t.nodeid = l.destid and t.isinput = 1)
	left join objects meters on meters.id = m.id
	union all
	select distinct m.id, t.attribute, t.metername, meters.sfname from ports p
	inner join cases c on 1=1 and c.id = %s
	inner join objects o on o.id = p.id and o.createcaseid <= c.id and (o.deletecaseid is null or o.deletecaseid > c.id)
	inner join flowsmeters fm on fm.flowid = p.flowid
	inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= c.id and (ofm.deletecaseid is null or ofm.deletecaseid > c.id)
	inner join objects m on m.id = fm.meterid
	inner join #checker_table t on (t.nodeid = p.unitid or t.nodeid = p.connectionobjid) and p.isinput = t.isinput
	left join objects meters on meters.id = m.id) data
	select c1.metername, c1.attribute, isnull(c1.sfname,'') from #checker_table2 c1
	inner join (
		select metername, count(*) c from #checker_table2 group by metername
	) counts on counts.metername = c1.metername
	where (counts.c > 1 and c1.sfname is not null) or counts.c = 1
	order by c1.metername, c1.attribute
	drop table #checker_table; drop table #checker_table2
	`, *caseid, *caseid)
}