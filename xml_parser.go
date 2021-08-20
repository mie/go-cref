package main

import (
	"fmt"
	"encoding/xml"
	"io/ioutil"
	"os"
	"strings"
)

type DataMap struct {
	XMLName xml.Name	`xml:"DataMap"`
	Tables []Table		`xml:"Table"`
}

type Table struct {
	XMLName xml.Name 	`xml:"Table"`
	DBName string		`xml:"dbTableName,attr"`
	Templates []Template	`xml:"Template"`
}

type Template struct {
	XMLName xml.Name	`xml:"Template"`
	Name string			`xml:"Name,attr"`
	Attributes []Attribute	`xml:"Attribute"`
}

type Attribute struct {
	XMLName xml.Name	`xml:"Attribute"`
	Analyse	string		`xml:"runInAnalyses,attr"`
	AfAttributes []Af	`xml:"Af"`
}

type Af struct {
	XMLName xml.Name	`xml:"Af"`
	Name string			`xml:"name,attr"`
}

type XMLParser struct {
	Tables []string
}

type AttributesMap struct {
	TableName string
	Attributes []string
}

func (xp *XMLParser) Parse(filename string, analyse string) []AttributesMap {
	xmlFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	defer xmlFile.Close()
	bContents, _ := ioutil.ReadAll(xmlFile)
	var d DataMap
	xml.Unmarshal(bContents, &d)

	var output []AttributesMap
	var a string
	var found bool
	var attrs []string

	for i := 0; i < len(d.Tables); i++ {
		attrs = nil
		for j := 0; j < len(d.Tables[i].Templates); j++ {
			for k := 0; k < len(d.Tables[i].Templates[j].Attributes); k++ {
				if (d.Tables[i].Templates[j].Attributes[k].Analyse == "" || strings.Contains(d.Tables[i].Templates[j].Attributes[k].Analyse, analyse)) {
					for l := 0; l < len(d.Tables[i].Templates[j].Attributes[k].AfAttributes); l++ {
						a = d.Tables[i].Templates[j].Attributes[k].AfAttributes[l].Name
						found = false
						for _, attr := range attrs {
							if a == attr {
								found = true
							}
						}
						if !found {
							attrs = append(attrs, a)
						}
					}	
				}
			}
		}
		if len(attrs) > 0 {
			var am AttributesMap
			xp.Tables = append(xp.Tables, d.Tables[i].DBName)
			am.TableName = d.Tables[i].DBName
			am.Attributes = attrs
			output = append(output, am)
		}
	}
	fmt.Printf("Loaded %d templates\n", len(xp.Tables))
	return output
}
