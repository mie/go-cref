package main

import (
	"fmt"
	"github.com/deckarep/golang-set"
	"io/ioutil"
	"errors"
)

type Node struct {
	name string
	deps []string
}

type Graph []*Node

func display(gr Graph) string {
	var out string
	for _, node := range gr {
		for _, dep := range node.deps {
			out = out + fmt.Sprintf("%s -> %s\n", node.name, dep)
		}
	}
	return out
}

func New_Node(name string, deps ...string) *Node {
	n := &Node {
		name: name,
		deps: deps,
	}
	return n
}

func (gr *Graph) insert(name string, deps ...string) {
	n := &Node {
		name: name,
		deps: deps,
	}
	*gr = append(*gr, n)
}

func resolve(gr Graph) (Graph, error) {
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
		// Get all nodes from the graph which have no dependencies
		readySet := mapset.NewSet()
		for name, deps := range dependencies {
			if deps.Cardinality() == 0 {
				readySet.Add(name)
			}
		}
		fmt.Println(readySet.Cardinality())

		// If there aren't any ready nodes, then we have a cicular dependency
		if readySet.Cardinality() == 0 {
			var g Graph
			out := ""
			for name := range dependencies {
				g = append(g, nodeSet[name])
				out = out + name + ":\n"
				for _, d := range nodeSet[name].deps {
					out = out + d + ","
				}
				out = out + "\n"
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