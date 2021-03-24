package main

import (
	"fmt"
	"flag"
	"errors"
	"regexp"
	"log"
	"io/ioutil"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/deckarep/golang-set"
)

type Node struct {
	name string
	deps []string
}

func New_Node(name string, deps ...string) *Node {
	n := &Node {
		name: name,
		deps: deps,
	}
	return n
}

type Graph []*Node

var graph Graph
var caseid *string
var db *sql.DB

func handle_formula(sfname *string, settings *string) {
	rf := regexp.MustCompile(`[A-Z]=\.*\\*([\s\p{L}\w\/\+\-]+)[\\|\|]MeasuredMass;`)
	depr := rf.FindAllStringSubmatch(*settings, -1)
	var deps []string
	for _, sm := range depr {
		deps = append(deps, sm[1])
	}
	n := New_Node(*sfname, deps...)
	graph = append(graph, n)
}

func handle_smartsum(sfname *string, settings *string) {
	rs := regexp.MustCompile(`\d+`)
	depr := rs.FindStringSubmatch(*settings)
	if len(depr) > 0 {
		res, err := db.Query(`
			select distinct m.sfname
			from links l
			inner join objects o on o.id = l.id and o.createcaseid <= @p1 and (o.deletecaseid is null or o.deletecaseid > @p1)
			inner join flowsmeters fm on fm.flowid = l.flowid
			inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= @p1 and (ofm.deletecaseid is null or ofm.deletecaseid > @p1)
			inner join objects m on m.id = fm.meterid
			where (l.sourceid = @p2 or l.destid = @p2) and m.sfname != @p3
			union all 
			select distinct m.sfname
			from ports p
			inner join objects o on o.id = p.id and o.createcaseid <= @p1 and (o.deletecaseid is null or o.deletecaseid > @p1)
			inner join flowsmeters fm on fm.flowid = p.flowid
			inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= @p1 and (ofm.deletecaseid is null or ofm.deletecaseid > @p1)
			inner join objects m on m.id = fm.meterid
			where p.unitid = @p2 and m.sfname != @p3`, *caseid, depr[0], *sfname)
		if err != nil {
			log.Fatal(err)
		}

		var meterid string
		var deps []string

		for res.Next() {
			err := res.Scan(&meterid)
			if err != nil {
				log.Fatal(err)
			}
			deps = append(deps, meterid)
		}
		
		n := New_Node(*sfname, deps...)
		graph = append(graph, n)
	}
}

func handle_tankmass(sfname *string, settings *string) {
	res, err := db.Query(`
		select distinct m.sfname
		from objects t
		inner join links l on l.destid = t.id or l.sourceid = t.id
		inner join objects o on o.id = l.id and o.createcaseid <= @p1 and (o.deletecaseid is null or o.deletecaseid > @p1)
		inner join flowsmeters fm on fm.flowid = l.flowid
		inner join objects ofm on ofm.id = fm.id and ofm.createcaseid <= @p1 and (ofm.deletecaseid is null or ofm.deletecaseid > @p1)
		inner join objects m on m.id = fm.meterid
		where t.sfname = @p2`, *caseid, *sfname)
	if err != nil {
		log.Fatal(err)
	}

	var meterid string
	var deps []string

	for res.Next() {
		err := res.Scan(&meterid)
		if err != nil {
			log.Fatal(err)
		}
		deps = append(deps, meterid)
	}
	
	n := New_Node(*sfname, deps...)
	graph = append(graph, n)
}

func displayGraph(graph Graph) {
	for _, node := range graph {
		for _, dep := range node.deps {
			fmt.Printf("%s -> %s\n", node.name, dep)
		}
	}
}

func resolve() (Graph, error) {
	nodeSet := make(map[string] *Node)
	dependencies := make(map[string] mapset.Set)
	fmt.Println("Populating dataset...")
	for _, node := range graph {
		nodeSet[node.name] = node

		dependencySet := mapset.NewSet()
		for _, dep := range node.deps {
			dependencySet.Add(dep)
		}
		dependencies[node.name] = dependencySet
	}

	var resolved Graph
	i := 0
	for len(dependencies) != 0 {
		i += 1
		fmt.Println("Pass", i)
		// Get all nodes from the graph which have no dependencies
		readySet := mapset.NewSet()
		for name, deps := range dependencies {
			if deps.Cardinality() == 0 {
				readySet.Add(name)
			}
		}

		// If there aren't any ready nodes, then we have a cicular dependency
		if readySet.Cardinality() == 0 {
			var g Graph
			out := ""
			for name := range dependencies {
				g = append(g, nodeSet[name])
				out = out + "\n" + name + ":\n"
				for _, d := range nodeSet[name].deps {
					out = out + d + ","
				}
			}
			err := ioutil.WriteFile("circle.txt", []byte(out), 0644)
		    if err != nil {
		        panic(err)
		    }
			return g, errors.New("Circular dependency found")
		}

		// Remove the ready nodes and add them to the resolved graph
		for name := range readySet.Iter() {
			delete(dependencies, name.(string))
			resolved = append(resolved, nodeSet[name.(string)])
		}

		// Also make sure to remove the ready nodes from the
		// remaining node dependencies as well
		for name, deps := range dependencies {
			diff := deps.Difference(readySet)
			dependencies[name] = diff
		}
	}

	return resolved, nil
}

func main() {
	caseid = flag.String("caseid", "", "caseid")

	flag.Parse()

	fmt.Println("Caseid:", *caseid)
	
	connstring := "server=localhost;user id=;trusted_connection=true;database=mb4;"

	var err error

	db, err = sql.Open("sqlserver", connstring)
	if err != nil {
		log.Fatal("couldn't connect")
	}

	fmt.Println("Connected to SQL Server")
	
	defer db.Close()

	res, err := db.Query(
		`select o.sfname, a.afreference, a.settings, a.settingsorigin
		from attrsettings a
		inner join objects o on o.sfid = a.afelement and o.modelsfid like 'f%' and o.createcaseid <= @p1 and (o.deletecaseid is null or o.deletecaseid > @p1)
		where a.isdeleted = 0 and a.afattribute = 'MeasuredMass' order by o.sfname`, *caseid)
	if err != nil {
		log.Fatal(err)	
	}

	fmt.Println("Initial data loaded. Parsing...")

	defer res.Close()

	var sfname, afreference, settings, settingsorigin string
	
	i:= 0

	for res.Next() {
		err := res.Scan(&sfname, &afreference, &settings, &settingsorigin)
		if err != nil {
			log.Fatal(err)
		}

		i += 1

		if afreference == "Formula" {
			handle_formula(&sfname, &settingsorigin)
		} else if afreference == "Smart Sum of Transfers" {
			handle_smartsum(&sfname, &settings)
		} else if afreference == "Tank Mass" {
			handle_tankmass(&sfname, &settings)
		} else {
			n := New_Node(sfname)
			graph = append(graph, n)
		}
	}

	fmt.Println("Parsing done.", i, "elements parsed.")
	fmt.Println("Calculating circular links...")

	_, err = resolve()
	if err != nil {
		fmt.Printf("Failed to resolve dependency graph: %s\n", err)
	} else {
		fmt.Println("The dependency graph resolved successfully")
	}

	
}