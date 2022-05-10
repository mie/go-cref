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
		GetCaseQuery(caseid))
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
	var smartsum []smart_sum
	var simplesum []smart_sum
	var tankmass []smart_sum

	var obj, settings, attribute, reference string

	var graph Graph

	// Formulas
	for _, a := range attrs {
		q := GetFormulasQuery(caseid, &a.Attributes, &a.TemplateName, &model)
		queries = append(queries, q)
	}

	res, err = db.Query(strings.Join(queries[:], " union all "))
	if err != nil {
		log.Fatal(err)
	}
	
	for res.Next() {
		err := res.Scan(&obj, &attribute, &reference, &settings)
		if err != nil {
			log.Fatal(err)
		}
		if reference == "Formula" {

			rf := regexp.MustCompile(`[A-Z]=\.?\.?\\?([^\|^\\^;]*)[\\|\|]*([^;]*);`)
			
			depr := rf.FindAllStringSubmatch(settings, -1)
			deps = nil
			for _, sm := range depr {
				if sm[2] == "" {
					deps = append(deps, obj + "|" + sm[1])
				} else {
					deps = append(deps, sm[1] + "|" + sm[2])
				}
			}
			insert(&graph, obj + "|" + attribute, deps...)
		} else if reference == "Smart Sum of Transfers" {
			rf := regexp.MustCompile(`\:(\d+)[,|\}]?`)
			depr := rf.FindAllStringSubmatch(settings, -1)
			for _, sm := range depr {
				smartsum = append(smartsum, smart_sum{objectid: obj, attribute: attribute, nodeid: sm[1]})
			}
		} else if reference == "Sum of Transfers" {
			rf := regexp.MustCompile(`(\d+)\\(True|False)\\`)
			depr := rf.FindAllStringSubmatch(settings, -1)
			for _, sm := range depr {
				if sm[2] == "True" {
					simplesum = append(simplesum, smart_sum{objectid: obj, attribute: attribute, nodeid: sm[1], input: true})
				} else {
					simplesum = append(simplesum, smart_sum{objectid: obj, attribute: attribute, nodeid: sm[1], input: false})
				}
				
			}
		} else if reference == "Tank Mass" {
			tankmass = append(tankmass, smart_sum{objectid: obj, attribute: attribute, nodeid: obj})
		} else {
			insert(&graph, obj + "|" + attribute)
		}
	}

	fmt.Println("Collected: formulas", len(graph))

	// Smart Sum of Transfers

	queries = nil

	for _, ss := range smartsum {
		q := fmt.Sprintf("insert into #checker_table values ('%s', '%s', %s)", ss.objectid, ss.attribute, ss.nodeid)
		queries = append(queries, q)
	}
	
	res, err = db.Query("create table #checker_table (metername nvarchar(255), attribute nvarchar(255),nodeid int); " + strings.Join(queries[:], "\n") + GetSmartSTQuery(caseid))

	if err != nil {
		log.Fatal(err)
	}

	var meterid, prev string
	deps = nil

	first_pass := true

	notLast := res.Next()

	for notLast {
		err := res.Scan(&meterid, &attribute, &obj)
		if err != nil {
			log.Fatal(err)
		}
		if first_pass {
			prev = meterid
			first_pass = false
		}
		if prev != meterid {
			if deps == nil {
				insert(&graph, prev + "|" + attribute)
			} else {
				insert(&graph, prev + "|" + attribute, deps...)
				deps = nil
			}
			prev = meterid
		}
		if obj != "" {
			deps = append(deps, obj + "|" + *measured)
		}
		notLast = res.Next()
		if !notLast {
			if deps == nil {
				insert(&graph, meterid + "|" + attribute)
			} else {
				insert(&graph, meterid + "|" + attribute, deps...)
			}
			
		}
	}

	fmt.Println("Collected: smart sum of transfers")

	// Sum of Transfers

	queries = nil

	for _, ss := range simplesum {
		var q string
		if ss.input == true {
			q = fmt.Sprintf("insert into #checker_table values ('%s', '%s', %s, 1)", ss.objectid, ss.attribute, ss.nodeid)
		} else {
			q = fmt.Sprintf("insert into #checker_table values ('%s', '%s', %s, 0)", ss.objectid, ss.attribute, ss.nodeid)
		}
		queries = append(queries, q)
	}

	
	res, err = db.Query("create table #checker_table (metername nvarchar(255), attribute nvarchar(255),nodeid int, isinput bit); " + strings.Join(queries[:], "\n") + GetSTQuery(caseid))
	if err != nil {
		log.Fatal(err)
	}

	deps = nil

	first_pass = true

	notLast = res.Next()
	for notLast {
		err := res.Scan(&meterid, &attribute, &obj)
		if err != nil {
			log.Fatal(err)
		}
		if first_pass {
			prev = meterid
			first_pass = false
		}
		if prev != meterid {
			if deps == nil {
				insert(&graph, prev + "|" + attribute)
			} else {
				insert(&graph, prev + "|" + attribute, deps...)
				deps = nil
			}
			prev = meterid
		}
		if obj != "" {
			deps = append(deps, obj + "|" + *measured)
		}
		notLast = res.Next()
		if !notLast {
			if deps == nil {
				insert(&graph, meterid + "|" + attribute)
			} else {
				insert(&graph, meterid + "|" + attribute, deps...)
			}
			
		}
	}
	

	fmt.Println("Collected: sum of transfers")
	
	// Tank Mass

	queries = nil

	for _, a := range tankmass {
		q := GetTankMassQuery(caseid, measured, &a.attribute, &a.objectid, &model)
		queries = append(queries, q)
	}
	if (queries != nil) {
		fmt.Println("select * from (" + strings.Join(queries[:], " union all ") + ") data order by sfname, afattribute");
	
		res, err = db.Query("select * from (" + strings.Join(queries[:], " union all ") + ") data order by sfname, afattribute")
		if err != nil {
			log.Fatal(err)
		}

		deps = nil
		first_pass = true

		notLast = res.Next()

		for notLast {
			err := res.Scan(&obj, &attribute, &settings)
			if err != nil {
				log.Fatal(err)
			}
			if first_pass {
				prev = obj
				first_pass = false
			}
			if prev != obj {
				insert(&graph, obj + "|" + attribute, deps...)
				prev = obj
				deps = nil
			}
			deps = append(deps, settings)

			notLast = res.Next()
			if !notLast {
				insert(&graph, obj + "|" + attribute, deps...)
			}
		}
	}
	

	

	fmt.Println("Collected: tank mass")

	queries = nil

	// Everything else

	for _, a := range attrs {
		q := GetTheRestQuery(caseid, &a.Attributes, &a.TemplateName, &model)
		queries = append(queries, q)
	}

	res, err = db.Query(strings.Join(queries[:], " union all "))
	if err != nil {
		log.Fatal(err)
	}
	
	deps = nil
	i := 0
	for res.Next() {
		err := res.Scan(&obj)
		if err != nil {
			log.Fatal(err)
		}
		insert(&graph, obj)
		i += 1
	}

	fmt.Println("Collected: the rest (", i, " items)")

	fmt.Println("Initial data loaded (", len(graph) ,") items Parsing...")

	fmt.Println("Parsing done.")
	fmt.Println("Calculating circular links...")
	filefy(graph.display(),"graph.txt")
	_, err = graph.resolve()
	if err != nil {
		fmt.Printf("Failed to resolve dependency graph: %s\n", err)
	} else {
		fmt.Println("The dependency graph resolved successfully")
	}

	
}