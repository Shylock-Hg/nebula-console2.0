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
	"strconv"
	"time"

	ngdb "github.com/shylock-hg/nebula-go2.0"
	common "github.com/shylock-hg/nebula-go2.0/nebula"
	graph "github.com/shylock-hg/nebula-go2.0/nebula/graph"
	"golang.org/x/crypto/ssh/terminal"
)

const NebulaLabel = "Nebula-Console"
const Version = "v2.0.0-alpha"

func welcome(interactive bool) {
	if !interactive {
		return;
	}
	fmt.Printf("Welcome to Nebula Graph %s!", Version)
	fmt.Println()
}

func bye(username string, interactive bool) {
	if !interactive {
		return;
	}
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

func val2String(value *common.Value, depth uint) string {
	// TODO(shylock) get golang runtime limit
	if depth == 0 {  // Avoid too deep recursive
		return ""
	}

	if value.IsSetNVal() {  // null
		switch value.GetNVal() {
		case common.NullType___NULL__:
			return "NULL"
		case common.NullType_NaN:
			return "NaN"
		case common.NullType_BAD_DATA:
			return "BAD_DATA"
		case common.NullType_BAD_TYPE:
			return "BAD_TYPE"
		}
	} else if value.IsSetBVal() {  // bool
		return strconv.FormatBool(value.GetBVal())
	} else if value.IsSetIVal() {  // int64
		return strconv.FormatInt(value.GetIVal(), 10)
	} else if value.IsSetFVal() {  // float64
		return strconv.FormatFloat(value.GetFVal(), 'g', -1, 64)
	} else if value.IsSetSVal() {  // string
		return "\"" + string(value.GetSVal()) + "\""
	} else if value.IsSetDVal() {  // yyyy-mm-dd
		date := value.GetDVal()
		str := fmt.Sprintf("%d-%d-%d", date.GetYear(), date.GetMonth(), date.GetDay())
		return str
	} else if value.IsSetTVal() {  // yyyy-mm-dd HH:MM:SS:MS TZ
		datetime := value.GetTVal()
		// TODO(shylock) timezone
		str := fmt.Sprintf("%d-%d-%d %d:%d:%d:%d",
			datetime.GetYear(), datetime.GetMonth(), datetime.GetDay(),
			datetime.GetHour(), datetime.GetMinute(), datetime.GetSec(), datetime.GetMicrosec())
		return str
	} else if value.IsSetVVal() {  // Vertex
		// VId only
		return string(value.GetVVal().GetVid())
	} else if value.IsSetEVal() {  // Edge
		// src-[TypeName]->dst@ranking
		edge := value.GetEVal()
		return fmt.Sprintf("%s-[%s]->%s@%d", string(edge.GetSrc()), edge.GetName(), string(edge.GetDst()),
			edge.GetRanking())
	} else if value.IsSetPVal() {  // Path
		// src-[TypeName]->dst@ranking-[TypeName]->dst@ranking ...
		p := value.GetPVal()
		str := string(p.GetSrc().GetVid())
		for _, step := range p.GetSteps() {
			pStr := fmt.Sprintf("-[%s]->%s@%d", step.GetName(), string(step.GetDst().GetVid()), step.GetRanking())
			str += pStr
		}
		return str
	} else if value.IsSetLVal() {  // List
		// TODO(shylock) optimize the recursive
		l := value.GetLVal()
		str := "["
		for _, v := range l.GetValues() {
			str += val2String(v, depth - 1)
			str += ","
		}
		str += "]"
		return str
	} else if value.IsSetMVal() {  // Map
		// TODO(shylock) optimize the recursive
		m := value.GetMVal()
		str := "{"
		for k, v := range m.GetKvs() {
			str += k
			str += ":"
			str += val2String(v, depth - 1)
			str += ","
		}
		str += "}"
		return str
	} else if value.IsSetUVal() {  // Set
		// TODO(shylock) optimize the recursive
		s := value.GetUVal()
		str := "{"
		for _, v := range s.GetValues() {
			str += val2String(v, depth - 1)
			str += ","
		}
		str += "}"
		return str
	}
	return ""
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
		colString := "|" + strings.Repeat(" ", align) + col;
		length := uint(len(col))
		if length < colSpec[i] + align {
			colString = colString + strings.Repeat(" ", int(colSpec[i]+align - length))
		}
		fmt.Print(colString)
	}
	fmt.Println("|")
}

func printTable(table *ngdb.DataSet) {
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
			tableRows[i][j] = val2String(col, 256)
			tableSpec[j] = max(uint(len(tableRows[i][j])), tableSpec[j])
		}
	}

	//                 value limit         + two indent              + '|' itself
	totalLineLength := int(sum(tableSpec)) + columnSize * align * 2  + columnSize + 1
	headerLine := strings.Repeat(headerChar, totalLineLength)
	rowLine := strings.Repeat(rowChar, totalLineLength)
	fmt.Println(headerLine)
	printRow(tableHeader, tableSpec)
	fmt.Println(headerLine)
	for _, row := range tableRows {
		printRow(row, tableSpec)
		fmt.Println(rowLine)
	}
	fmt.Printf("Got %d rows, %d columns.", rowSize, columnSize)
	fmt.Println()
}

func printResp(resp *graph.ExecutionResponse, duration time.Duration) {
	// Error
	if resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED {
		fmt.Printf("[ERROR (%d)]", resp.GetErrorCode())
		fmt.Println()
		return
	}
	// Show tables
	if resp.GetData() != nil {
		for _, table := range resp.GetData() {
			printTable(table)
		}
	}
	// Show time
	fmt.Printf("time spent %d/%d us", resp.GetLatencyInUs(), duration/*ns*//1000)
	fmt.Println()
}

const ttyColorPrefix = "\033["
const ttyColorSuffix = "m"
const ttyColorRed = "31"
const ttyColorBold = "1"
const ttyColorReset = "0"

// Space name
// Is error
func prompt(space string, user string, isErr bool, isTTY bool) {
	fmt.Println()
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
func loop(client *ngdb.GraphClient, input io.Reader, interactive bool, user string) {
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
			if !interactive {
				// Quit
				break
			}
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

		start := time.Now()
		resp, err := client.Execute(lineString)
		duration := time.Since(start)
		if err != nil {
			// Exception
			log.Fatalf("Execute error, %s", err.Error())
		}
		printResp(resp, duration)
		log.Print() // time
		currentSpace = string(resp.SpaceName)
		if interactive {
			prompt(currentSpace, user, resp.GetErrorCode() != graph.ErrorCode_SUCCEEDED, isTTY)
		}
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

	client, err := ngdb.NewClient(fmt.Sprintf("%s:%d", *address, *port))
	if err != nil {
		log.Fatalf("Fail to create client, address: %s, port: %d, %s", *address, *port, err.Error())
	}

	if err = client.Connect(*username, *password); err != nil {
		log.Fatalf("Fail to connect server, username: %s, password: %s, %s", *username, *password, err.Error())
	}

	welcome(interactive)

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

	bye(*username, interactive)

	client.Disconnect()
}
