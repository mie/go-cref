package main

import (
	"fmt"
	"github.com/deckarep/golang-set"
	"io/ioutil"
	"errors"
	"strings"
)

type Node struct {
	name string
	deps []string
}

type Graph []*Node

func (gr Graph) display() string {
	var out strings.Builder
	for _, node := range gr {
		if len(node.deps) == 0 {
			out.WriteString(fmt.Sprintf("%s -> \n", node.name))
		} else {
			for _, dep := range node.deps {
				out.WriteString(fmt.Sprintf("%s -> %s\n", node.name, dep))
			}
		}
	}
	return out.String()
}

func insert(gr *Graph, name string, deps ...string) {
	n := &Node {
		name: name,
		deps: deps,
	}
	*gr = append(*gr, n)
}

func (gr Graph) resolve() (Graph, error) {
	nodeSet := make(map[string] *Node)
	dependencies := make(map[string] mapset.Set)
	fmt.Println("Populating dataset...")
	for _, node := range gr {
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
		
		readySet := mapset.NewSet()
		for name, deps := range dependencies {
			if deps.Cardinality() == 0 {
				readySet.Add(name)
			}
		}
		fmt.Println(readySet.Cardinality())

		if readySet.Cardinality() == 0 {
			var g Graph
			var out strings.Builder
			for name := range dependencies {
				g = append(g, nodeSet[name])
				out.WriteString(name + ":\n")
				for _, d := range nodeSet[name].deps {
					out.WriteString(d + ",")
				}
				out.WriteString("\n")
			}
			err := ioutil.WriteFile("circle.txt", []byte(out.String()), 0644)
		    if err != nil {
		        panic(err)
		    }
			return g, errors.New("Circular dependency found")
		}

		for name := range readySet.Iter() {
			delete(dependencies, name.(string))
			resolved = append(resolved, nodeSet[name.(string)])
		}

		for name, deps := range dependencies {
			diff := deps.Difference(readySet)
			dependencies[name] = diff
		}
	}

	return resolved, nil
}