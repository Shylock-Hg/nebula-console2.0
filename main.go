/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	Graph "github.com/shylock-hg/nebula-go2.0"
	"golang.org/x/crypto/ssh/terminal"
)

const NebulaLabel = "Nebula-Console"
const Version = "v2.0.0-alpha"

func welcome() {
	fmt.Printf("Welcome to Nebula Graph %s!", Version)
	fmt.Println()
}

func bye(username string) {
	fmt.Printf("Bye %s!", username)
	fmt.Println()
}

// return , does exit
func clientCmd(query string) bool {
	// TODO(shylock) handle blank
	if query == "exit" || query == "quit" {
		return true
	}
	return false
}

// TODO(shylock) package the table visualization to class in sparate file

func val2String(value *Graph.Value) string {
	return value.String()
}

func max(v1 uint, v2 uint) uint {
	if v1 > v2 {
		return v1
	}
	return v2
}

func sum(a []uint) uint {
	s := uint(0)
	for _, v := range a {
		s += v
	}
	return s
}

// Columns width
type TableSpec = []uint
type TableRows = [][]string

const align = 2          // Each column align indent to boundary
const headerChar = "="   // Header line characters
const rowChar = "-"      // Row line characters
const colDelimiter = "|" // Column delemiter

func printRow(row []string, colSpec TableSpec) {
	for i, col := range row {
		// TODO(shylock) hard code the indent 2
		colString := fmt.Sprintf("|  %s", col)
		length := uint(len(colString))
		if length < colSpec[i] {
			colString = colString + strings.Repeat(" ", int(colSpec[i]-length+align))
		}
		fmt.Print(colString)
	}
	fmt.Println("|")
}

func printTable(table *Graph.DataSet) {
	columnSize := len(table.GetColumnNames())
	rowSize := len(table.GetRows())
	tableSpec := make(TableSpec, columnSize)
	tableRows := make(TableRows, rowSize)
	tableHeader := make([]string, columnSize)
	for i, header := range table.GetColumnNames() {
		tableSpec[i] = uint(len(header))
		tableHeader[i] = string(header)
	}
	for i, row := range table.GetRows() {
		tableRows[i] = make([]string, columnSize)
		for j, col := range row.GetColumns() {
			tableRows[i][j] = val2String(col)
			tableSpec[j] = max(uint(len(tableRows[i][j])), tableSpec[j])
		}
	}

	// print
	totalLineLength := sum(tableSpec) + uint(len(tableSpec)*align*2)
	headerLine := strings.Repeat(headerChar, int(totalLineLength))
	rowLine := strings.Repeat(rowChar, int(totalLineLength))
	fmt.Print(headerLine)
	printRow(tableHeader, tableSpec)
	fmt.Print(headerLine)
	for _, row := range tableRows {
		printRow(row, tableSpec)
		fmt.Print(rowLine)
	}
}

func printResp(resp *Graph.ExecutionResponse) {
	// Error
	if resp.GetErrorCode() != 0 {
		fmt.Printf("[ERROR (%d)]", resp.GetErrorCode())
		return
	}
	// Show tables
	if resp.GetData() != nil {
		for _, table := range resp.GetData() {
			printTable(table)
		}
	}
}

const ttyColorPrefix = "\033["
const ttyColorSuffix = "m"
const ttyColorRed = "31"
const ttyColorBold = "1"
const ttyColorReset = "0"

// Space name
// Is error
func prompt(space string, user string, isErr bool, isTTY bool) {
	// (user@nebula) [(space)] >
	if isTTY {
		fmt.Printf("%s%s%s", ttyColorPrefix, ttyColorBold, ttyColorSuffix)
	}
	if isTTY && isErr {
		fmt.Printf("%s%s%s", ttyColorPrefix, ttyColorRed, ttyColorSuffix)
	}
	fmt.Printf("(%s@%s) [(%s)]> ", user, NebulaLabel, space)
	if isTTY {
		fmt.Printf("%s%s%s", ttyColorPrefix, ttyColorReset, ttyColorSuffix)
	}
}

// Loop the request util fatal or timeout
// We treat one line as one query
// Add line break yourself as `SHOW \<CR>HOSTS`
func loop(client *Graph.GraphClient, input io.Reader, interactive bool, user string) {
	isTTY := terminal.IsTerminal(int(os.Stdout.Fd()))
	if interactive {
		prompt("", user, false, isTTY)
	}
	reader := bufio.NewReader(input)
	currentSpace := ""
	for true {
		line, _, err := reader.ReadLine()
		lineString := string(line)
		if err != nil {
			log.Printf("Get line failed: ", err.Error())
			if interactive {
				prompt(currentSpace, user, true, isTTY)
			}
			continue
		}
		if len(line) == 0 {
			// Empty line
			if interactive {
				prompt(currentSpace, user, false, isTTY)
			}
			continue
		}

		// Client side command
		if clientCmd(lineString) {
			// Quit
			break
		}

		resp, err := client.Execute(lineString)
		if err != nil {
			// Exception
			log.Fatalf("Execute error, %s", err.Error())
		}
		// TODO(shylock) hard code error code
		currentSpace = string(resp.SpaceName)
		if interactive {
			prompt(currentSpace, user, resp.GetErrorCode() != 0, isTTY)
		}
		printResp(resp)
		log.Print() // time
	}
}

func main() {
	address := flag.String("address", "127.0.0.1", "The Nebula Graph IP address")
	port := flag.Int("port", 3699, "The Nebula Graph Port")
	username := flag.String("username", "user", "The Nebula Graph login user name")
	password := flag.String("password", "password", "The Nebula Graph login password")
	script := flag.String("e", "", "The nGQL directly")
	file := flag.String("f", "", "The nGQL script file name")
	flag.Parse()

	interactive := *script == "" && *file == ""

	client, err := Graph.NewClient(fmt.Sprintf("%s:%d", *address, *port))
	if err != nil {
		log.Fatalf("Fail to create client, address: %s, port: %d, %s", *address, *port, err.Error())
	}

	if err = client.Connect(*username, *password); err != nil {
		log.Fatalf("Fail to connect server, username: %s, password: %s, %s", *username, *password, err.Error())
	}

	welcome()

	// Loop the request
	if interactive {
		loop(client, os.Stdin, interactive, *username)
	} else if *script != "" {
		loop(client, strings.NewReader(*script), interactive, *username)
	} else if *file != "" {
		fd, err := os.Open(*file)
		if err != nil {
			log.Fatalf("Open file %s failed, %s", *file, err.Error())
		}
		loop(client, fd, interactive, *username)
	}

	bye(*username)

	client.Disconnect()
}
