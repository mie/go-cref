package main

import (
	"fmt"
	"flag"
	"io/ioutil"
	"regexp"
	"log"
	"strings"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"

)

type smart_sum struct {
	objectid string
	attribute string
	nodeid string
	input bool
}

var caseid, measured *string
var db *sql.DB

func filefy(data string, file string) {
	err := ioutil.WriteFile(file, []byte(data), 0644)
    if err != nil {
        panic(err)
    }
}

func main() {
	// TODO: get real last caseid
	caseid = flag.String("caseid", "(select top 1 id from cases order by id desc)", "caseid")
	measured = flag.String("measured", "MeasuredMass", "measured")

	flag.Parse()

	connstring := "server=localhost;user id=;trusted_connection=true;database=mb4;"

	var err error

	db, err = sql.Open("sqlserver", connstring)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to SQL Server")
	
	defer db.Close()

	res, err := db.Query(
		`select c.id, a.name, m.name from cases c
		inner join analyses a on a.sfid = c.analysissfid
		inner join models m on m.sfid = a.modelsfid
		where c.id = ` + *caseid)
	if err != nil {
		log.Fatal(err)
	}

	defer res.Close()

	var analysis, model string

	fmt.Println("Caseid:", *caseid)
	fmt.Println("Measured AF Attribute:", *measured)
	for res.Next() {
		err := res.Scan(&caseid, &analysis, &model)
		fmt.Printf("Model: %s, Analysis: %s\n", model, analysis)
		if err != nil {
			log.Fatal(err)
		}
	}

	var x XMLParser
	var attrs []AttributesMap
	
	attrs = x.Parse("MappingConfig.xml", analysis)

	var queries []string
	var deps []string

	var obj, settings, attribute string

	var graph Graph

	// Formulas
	for _, a := range attrs {
		q := fmt.Sprintf(`select cast(o.sfname as nvarchar(255)) + '|' + a.afattribute, a.settingsorigin from %s r
			inner join objects o on o.id = r.id
			inner join models m on m.sfid = o.modelsfid
			inner join attrsettings a on a.afelement = o.sfid and a.isdeleted = 0 and a.afattribute in ('%s')
			where r.caseid = %s and a.afreference = 'Formula' and m.name = '%s'`, a.TableName, strings.Join(a.Attributes[:], "','"), *caseid, model)
		queries = append(queries, q)
	}

	res, err = db.Query(strings.Join(queries[:], " union all "))
	if err != nil {
		log.Fatal(err)
	}

	for res.Next() {
		err := res.Scan(&obj, &settings)
		if err != nil {
			log.Fatal(err)
		}
		// rf := regexp.MustCompile(`[A-Z]=(\d+)\\[^\\]+\\([^;]+);`)
		rf := regexp.MustCompile(`[A-Z]=\.*\\*([\s\p{L}\w\/\+\-]+)[\\|\|]([^;]+);`)
		depr := rf.FindAllStringSubmatch(settings, -1)
		deps = nil
		for _, sm := range depr {
			deps = append(deps, sm[1] + "|" + sm[2])
		}
		graph.insert(obj, deps...)
	}

	fmt.Println("Collected: formulas")

	queries = nil

	// Smart Sum of Transfers

	for _, a := range attrs {
		q := fmt.Sprintf(`select o.sfname, a.afattribute, a.settings from %s r
		inner join objects o on o.id = r.id
		inner join models m on m.sfid = o.modelsfid
		inner join attrsettings a on a.afelement = o.sfid and a.isdeleted = 0 and a.afattribute in ('%s')
		where r.caseid = %s and a.afreference = 'Smart Sum of Transfers' and m.name = '%s'`, a.TableName, strings.Join(a.Attributes[:], "','"), *caseid, model)
		queries = append(queries, q)
	}

	res, err = db.Query(strings.Join(queries[:], " union all "))
	if err != nil {
		log.Fatal(err)
	}

	var nodes []smart_sum

	for res.Next() {
		err := res.Scan(&obj, &attribute, &settings)
		
		if err != nil {
			log.Fatal(err)
		}
		rf := regexp.MustCompile(`(\d+)[,|\}]?`)
		depr := rf.FindAllStringSubmatch(settings, -1)
		for _, sm := range depr {
			nodes = append(nodes, smart_sum{objectid: obj, attribute: attribute, nodeid: sm[1]})
		}
	}

	queries = nil

	for _, ss := range nodes {
		q := fmt.Sprintf("insert into #checker_table values ('%s', '%s', %s)", ss.objectid, ss.attribute, ss.nodeid)
		queries = append(queries, q)
	}
	
	res, err = db.Query("create table #checker_table (metername nvarchar(255), attribute nvarchar(255),nodeid int); " + strings.Join(queries[:], "\n") + fmt.Sprintf(`
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
		inner join #checker_table t on t.nodeid = p.unitid
		left join objects meters on meters.sfname != t.metername and meters.id = m.id) data
		select c1.metername, c1.attribute, isnull(c1.sfname, '') from #checker_table2 c1
		inner join (
			select metername, count(*) c from #checker_table2 group by metername
		) counts on counts.metername = c1.metername
		where (counts.c > 1 and c1.sfname is not null) or counts.c = 1
		order by metername
		drop table #checker_table; drop table #checker_table2`, *caseid, *caseid))
	if err != nil {
		log.Fatal(err)
	}

	var meterid, prev string
	deps = nil

	first_pass := true

	for res.Next() {
		err := res.Scan(&meterid, &attribute, &obj)
		if err != nil {
			log.Fatal(err)
		}
		if first_pass {
			prev = meterid
			first_pass = false
		} else {
			if prev == meterid {
				if obj != "" {
					deps = append(deps, obj + "|" + *measured)
				}
			} else {
				graph.insert(meterid + "|" + attribute, deps...)
				prev = meterid
				deps = nil
			}
			if !res.Next() {
				graph.insert(meterid + "|" + attribute, deps...)
			}
		}
	}
	

	fmt.Println("Collected: smart sum of transfers")

	queries = nil

	// Sum of Transfers

	for _, a := range attrs {
		q := fmt.Sprintf(`select o.sfname, a.afattribute, a.settings from %s r
		inner join objects o on o.id = r.id
		inner join models m on m.sfid = o.modelsfid
		inner join attrsettings a on a.afelement = o.sfid and a.isdeleted = 0 and a.afattribute in ('%s')
		where r.caseid = %s and a.afreference = 'Sum of Transfers' and m.name = '%s'`, a.TableName, strings.Join(a.Attributes[:], "','"), *caseid, model)
		queries = append(queries, q)
	}

	res, err = db.Query(strings.Join(queries[:], " union all "))
	if err != nil {
		log.Fatal(err)
	}

	nodes = nil

	for res.Next() {
		err := res.Scan(&obj, &attribute, &settings)
		
		if err != nil {
			log.Fatal(err)
		}
		rf := regexp.MustCompile(`(\d+)\\(True|False)\\`)
		depr := rf.FindAllStringSubmatch(settings, -1)
		for _, sm := range depr {
			if sm[2] == "True" {
				nodes = append(nodes, smart_sum{objectid: obj, attribute: attribute, nodeid: sm[1], input: true})
			} else {
				nodes = append(nodes, smart_sum{objectid: obj, attribute: attribute, nodeid: sm[1], input: false})
			}
			
		}
	}

	queries = nil

	for _, ss := range nodes {
		var q string
		if ss.input == true {
			q = fmt.Sprintf("insert into #checker_table values ('%s', '%s', %s, 1)", ss.objectid, ss.attribute, ss.nodeid)
		} else {
			q = fmt.Sprintf("insert into #checker_table values ('%s', '%s', %s, 0)", ss.objectid, ss.attribute, ss.nodeid)
		}
		queries = append(queries, q)
	}

	
	res, err = db.Query("create table #checker_table (metername nvarchar(255), attribute nvarchar(255),nodeid int, isinput bit); " + strings.Join(queries[:], "\n") + fmt.Sprintf(`
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
		inner join #checker_table t on t.nodeid = p.unitid and p.isinput = t.isinput
		left join objects meters on meters.id = m.id) data
		select c1.metername, c1.attribute, isnull(c1.sfname,'') from #checker_table2 c1
		inner join (
			select metername, count(*) c from #checker_table2 group by metername
		) counts on counts.metername = c1.metername
		where (counts.c > 1 and c1.sfname is not null) or counts.c = 1
		order by metername
		drop table #checker_table; drop table #checker_table2
		`, *caseid, *caseid))
	if err != nil {
		log.Fatal(err)
	}

	deps = nil

	first_pass = true

	for res.Next() {
		err := res.Scan(&meterid, &attribute, &obj)
		if err != nil {
			log.Fatal(err)
		}
		if first_pass {
			prev = meterid
			first_pass = false
		} else {
			if meterid == prev {
				if obj == "" {
					deps = append(deps, obj + "|" + *measured)
				}
			} else {
				graph.insert(meterid + "|" + attribute, deps...)
				prev = meterid
				deps = nil
			}
			if !res.Next() {
				graph.insert(meterid + "|" + attribute, deps...)
			}
		}
	}
	

	fmt.Println("Collected: sum of transfers")
	
	// Tank Mass

	queries = nil

	for _, a := range attrs {
		q := fmt.Sprintf(`select o.sfname, a.afattribute, cast(m.sfname as nvarchar(20)) + '|%s' s from %s r
		inner join objects o on o.id = r.id
		inner join models md on md.sfid = o.modelsfid
		inner join attrsettings a on a.afelement = o.sfid and a.isdeleted = 0 and a.afattribute in ('%s')
		inner join links l on l.destid = o.id or l.sourceid = o.id
		inner join cases c on 1=1 and c.id = %s
		inner join objects ol on ol.id = l.id and ol.createcaseid <= c.id and (ol.deletecaseid is null or ol.deletecaseid > c.id)
		inner join flowsmeters fm on fm.flowid = l.flowid
		inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= c.id and (ofm.deletecaseid is null or ofm.deletecaseid > c.id)
		inner join objects m on m.id = fm.meterid
		where r.caseid = %s and a.afreference = 'Tank Mass' and md.name = '%s'`, *measured, a.TableName, strings.Join(a.Attributes[:], "','"), *caseid, *caseid, model)
		queries = append(queries, q)
	}
	
	res, err = db.Query("select * from (" + strings.Join(queries[:], " union all ") + ") data order by sfname, afattribute")
	if err != nil {
		log.Fatal(err)
	}

	deps = nil
	first_pass = true

	for res.Next() {
		err := res.Scan(&obj, &attribute, &settings)
		if err != nil {
			log.Fatal(err)
		}
		if first_pass {
			prev = obj
			first_pass = false
		} else {
			if prev == obj {
				deps = append(deps, settings)
			} else {
				graph.insert(obj + "|" + attribute, deps...)
				prev = obj
				deps = nil
			}
			if !res.Next() {
				graph.insert(obj + "|" + attribute, deps...)
			}
		}
	}

	

	fmt.Println("Collected: tank mass")

	queries = nil

	// Everything else

	for _, a := range attrs {
		q := fmt.Sprintf(`select cast(o.sfname as nvarchar(255)) + '|' + a.afattribute attr from %s r
		inner join objects o on o.id = r.id
		inner join models m on m.sfid = o.modelsfid
		inner join attrsettings a on a.afelement = o.sfid and a.isdeleted = 0 and a.afattribute in ('%s')
		where r.caseid = %s and not a.afreference in ('Formula','Smart Sum of Transfers','Sum of Transfers','Tank Mass') and m.name = '%s'`, a.TableName, strings.Join(a.Attributes[:], "','"), *caseid, model)
		queries = append(queries, q)
	}

	res, err = db.Query(strings.Join(queries[:], " union all "))
	if err != nil {
		log.Fatal(err)
	}
	
	deps = deps[:0]
	i := 0
	for res.Next() {
		err := res.Scan(&obj)
		if err != nil {
			log.Fatal(err)
		}
		n := &Node {
			name: obj,
			deps: deps,
		}
		graph = append(graph, n)
		i += 1
	}

	fmt.Println("Collected: the rest (", i, " items)")

	fmt.Println("Initial data loaded. Parsing...")

	fmt.Println("Parsing done.")
	fmt.Println("Calculating circular links...")
	fmt.Println(len(graph))
	_, err = resolve(graph)
	if err != nil {
		fmt.Printf("Failed to resolve dependency graph: %s\n", err)
	} else {
		fmt.Println("The dependency graph resolved successfully")
	}

	
}