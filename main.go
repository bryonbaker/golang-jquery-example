package main

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"

	"github.com/itchyny/gojq"
)

const msg string = "{\"header\":{\"id\":\"e1f4f65c-27df-418a-accd-f3b62c71ef02\",\"test\":false,\"prepared\":\"2022-09-04T06:43:37.894+02:00\",\"sender\":{\"id\":\"ECB\"}},\"dataSets\":[{\"action\":\"Replace\",\"validFrom\":\"2022-09-04T06:43:37.894+02:00\",\"series\":{\"0:0:0:0:0\":{\"attributes\":[0,null,0,null,null,null,null,null,null,null,null,null,0,null,0,null,0,0,0,0],\"observations\":{\"0\":[1.4529,0,0,null,null],\"1\":[1.4472,0,0,null,null],\"2\":[1.4591,0,0,null,null],\"3\":[1.4651,0,0,null,null],\"4\":[1.4671,0,0,null,null]}}}}],\"structure\":{\"links\":[{\"title\":\"Exchange Rates\",\"rel\":\"dataflow\",\"href\":\"https://sdw-wsrest.ecb.europa.eu:443/service/dataflow/ECB/EXR/1.0\"}],\"name\":\"Exchange Rates\",\"dimensions\":{\"series\":[{\"id\":\"FREQ\",\"name\":\"Frequency\",\"values\":[{\"id\":\"D\",\"name\":\"Daily\"}]},{\"id\":\"CURRENCY\",\"name\":\"Currency\",\"values\":[{\"id\":\"AUD\",\"name\":\"Australian dollar\"}]},{\"id\":\"CURRENCY_DENOM\",\"name\":\"Currency denominator\",\"values\":[{\"id\":\"EUR\",\"name\":\"Euro\"}]},{\"id\":\"EXR_TYPE\",\"name\":\"Exchange rate type\",\"values\":[{\"id\":\"SP00\",\"name\":\"Spot\"}]},{\"id\":\"EXR_SUFFIX\",\"name\":\"Series variation - EXR context\",\"values\":[{\"id\":\"A\",\"name\":\"Average\"}]}],\"observation\":[{\"id\":\"TIME_PERIOD\",\"name\":\"Time period or range\",\"role\":\"time\",\"values\":[{\"id\":\"2022-08-29\",\"name\":\"2022-08-29\",\"start\":\"2022-08-29T00:00:00.000+02:00\",\"end\":\"2022-08-29T23:59:59.999+02:00\"},{\"id\":\"2022-08-30\",\"name\":\"2022-08-30\",\"start\":\"2022-08-30T00:00:00.000+02:00\",\"end\":\"2022-08-30T23:59:59.999+02:00\"},{\"id\":\"2022-08-31\",\"name\":\"2022-08-31\",\"start\":\"2022-08-31T00:00:00.000+02:00\",\"end\":\"2022-08-31T23:59:59.999+02:00\"},{\"id\":\"2022-09-01\",\"name\":\"2022-09-01\",\"start\":\"2022-09-01T00:00:00.000+02:00\",\"end\":\"2022-09-01T23:59:59.999+02:00\"},{\"id\":\"2022-09-02\",\"name\":\"2022-09-02\",\"start\":\"2022-09-02T00:00:00.000+02:00\",\"end\":\"2022-09-02T23:59:59.999+02:00\"}]}]},\"attributes\":{\"series\":[{\"id\":\"TIME_FORMAT\",\"name\":\"Time format code\",\"values\":[{\"name\":\"P1D\"}]},{\"id\":\"BREAKS\",\"name\":\"Breaks\",\"values\":[]},{\"id\":\"COLLECTION\",\"name\":\"Collection indicator\",\"values\":[{\"id\":\"A\",\"name\":\"Average of observations through period\"}]},{\"id\":\"COMPILING_ORG\",\"name\":\"Compiling organisation\",\"values\":[]},{\"id\":\"DISS_ORG\",\"name\":\"Data dissemination organisation\",\"values\":[]},{\"id\":\"DOM_SER_IDS\",\"name\":\"Domestic series ids\",\"values\":[]},{\"id\":\"PUBL_ECB\",\"name\":\"Source publication (ECB only)\",\"values\":[]},{\"id\":\"PUBL_MU\",\"name\":\"Source publication (Euro area only)\",\"values\":[]},{\"id\":\"PUBL_PUBLIC\",\"name\":\"Source publication (public)\",\"values\":[]},{\"id\":\"UNIT_INDEX_BASE\",\"name\":\"Unit index base\",\"values\":[]},{\"id\":\"COMPILATION\",\"name\":\"Compilation\",\"values\":[]},{\"id\":\"COVERAGE\",\"name\":\"Coverage\",\"values\":[]},{\"id\":\"DECIMALS\",\"name\":\"Decimals\",\"values\":[{\"id\":\"4\",\"name\":\"Four\"}]},{\"id\":\"NAT_TITLE\",\"name\":\"National language title\",\"values\":[]},{\"id\":\"SOURCE_AGENCY\",\"name\":\"Source agency\",\"values\":[{\"id\":\"4F0\",\"name\":\"European Central Bank (ECB)\"}]},{\"id\":\"SOURCE_PUB\",\"name\":\"Publication source\",\"values\":[]},{\"id\":\"TITLE\",\"name\":\"Title\",\"values\":[{\"name\":\"Australian dollar/Euro\"}]},{\"id\":\"TITLE_COMPL\",\"name\":\"Title complement\",\"values\":[{\"name\":\"ECB reference exchange rate, Australian dollar/Euro, 2:15 pm (C.E.T.)\"}]},{\"id\":\"UNIT\",\"name\":\"Unit\",\"values\":[{\"id\":\"AUD\",\"name\":\"Australian dollar\"}]},{\"id\":\"UNIT_MULT\",\"name\":\"Unit multiplier\",\"values\":[{\"id\":\"0\",\"name\":\"Units\"}]}],\"observation\":[{\"id\":\"OBS_STATUS\",\"name\":\"Observation status\",\"values\":[{\"id\":\"A\",\"name\":\"Normal value\"}]},{\"id\":\"OBS_CONF\",\"name\":\"Observation confidentiality\",\"values\":[{\"id\":\"F\",\"name\":\"Free\"}]},{\"id\":\"OBS_PRE_BREAK\",\"name\":\"Pre-break observation value\",\"values\":[]},{\"id\":\"OBS_COM\",\"name\":\"Observation comment\",\"values\":[]}]}}}"

// Interface spec: https://github.com/sdmx-twg/sdmx-json/blob/master/data-message/docs/1-sdmx-json-field-guide.md
// The FX rates are a list contained in: "dataSets"[0]."series"."0.0.0.0.0"."observations".

// "dataSets" is an array of "data" or "component" objects that contains the for the structure. If this is
// ommitted then all data in the message is assumed to be described by "structure".

// "series" is an array of "component "objects. Optional array to be provided if components
// (only dimensions or attributes) are presented at the series level.

// "0.0.0.0.0" represents a single dimension. This is used to describe the combination of
// "dimensions" from the "structure" attribute that make up the observation. It is a json
// way to describe the vectors in n-dimensional space that make up the observation.

// "component" represents a "dimension", a "measure", or an "attribute"

// Key data to extract. Format: "attribute: jquery path"
// FX Data Provider: .header.sender.id
// FX Rates: '.dataSets[0].series."0:0:0:0:0".observations'
// FX Rates for first observation: '.dataSets[0].series."0:0:0:0:0".observations."0"[0]'

func main() {
	// Load the raw json message
	var input map[string]interface{}
	json.Unmarshal([]byte(msg), &input)

	testQueries := []string{
		//		".header.sender.id",
		//		".header.sengder.id",
		//		".dataSets[0].series.\"0:0:0:0:0\".observations.\"0\"[0]",
		//		".dataSets[0].series.\"0:0:0:0:0\".observations",
		".structure.dimensions.series"}

	for _, v := range testQueries {

		json.Unmarshal([]byte(msg), &input)
		runQuery(&input, v)
	}

	parseResponse(msg)
}

// parseResponse extracts the FX details from the repsonse and stores it in a usable format
// that is base don the CSV formst you can download form ECB.
func parseResponse(ecbJsonResp string) {
	// A list of all the JQueries that are used.
	queries := map[string]string{
		"sender":                     ".header.sender.id",
		"time-period":                ".dataSets[0].validFrom",
		"count-observations":         ".dataSets[].series.\"0:0:0:0:0\".observations | length",
		"firstObservation":           ".dataSets[0].series.\"0:0:0:0:0\".observations.\"0\"[0]",
		"allObservations":            ".dataSets[0].series.\"0:0:0:0:0\".observations",
		"structure-observations-len": ".structure.dimensions.series | length,",
		"structure-observations":     ".structure.dimensions.series[0].",
		"dimension-frequency":        ".structure.dimensions.series[] | select(.id==\"FREQ\")",
		"frequency":                  ".structure.dimensions.series[] | select(.id==\"FREQ\") | .values[0].id"}

	// Run all thew JQueries to extract the data
	var input map[string]interface{}
	json.Unmarshal([]byte(ecbJsonResp), &input)

	var jsonVal interface{}

	jsonVal = queryPath(&input, queries["sender"])
	fmt.Println(jsonVal.(string))

	// Get the time of the value
	jsonVal = queryPath(&input, queries["time-period"])
	fmt.Println(jsonVal.(string))

	// Get the time of the value
	jsonVal = queryPath(&input, queries["frequency"])
	fmt.Println(jsonVal.(string))

	// Extract CURRENCY_DENOM
	queryDimensions(&input, "CURRENCY_DENOM")

	jsonVal = queryPath(&input, queries["count-observations"])
	fmt.Printf("Number of observations: %d\n", jsonVal.(int))

	return
}

func runQuery(input *map[string]interface{}, queryString string) {
	fmt.Printf("\nRunning JSON Query: %s\n", queryString)

	query, err := gojq.Parse(queryString)

	if err != nil {
		log.Fatalln(err)
	}

	iter := query.Run(*input) // or query.RunWithContext
	fmt.Printf("Iter type: %v\n", reflect.TypeOf(iter))

	for v, more := iter.Next(); more; v, more = iter.Next() {
		if err, more := v.(error); more {
			log.Fatalln(err)
		} else if v == nil {
			log.Printf("query returns no result: <%s>", queryString)
		} else {
			fmt.Printf("%#v\n", v)
			fmt.Printf("Type: %s\n", reflect.TypeOf(v))

			if reflect.TypeOf(v).Kind() == reflect.Map {
				// Cast the object
				m := v.(map[string]interface{})
				fmt.Println("Number of observations: ", len(m))
				fmt.Printf("Preview of the observations: %s\n", gojq.Preview(m))

				// PLay with getting an observation
				o := "0"
				fmt.Printf("Observation %s: %v\n", o, m[o])
				fmt.Printf("Observation type: %v\n", reflect.TypeOf(m[o]))

				// Play with extracting values from the observations
				a := m[o].([]interface{})
				fmt.Println((a[0]).(float64))
				val := (a[0]).(float64)
				s := strconv.FormatFloat(a[0].(float64), 'f', -1, 64)
				fmt.Println(s)
				fmt.Println(reflect.TypeOf(val))
			} else if reflect.TypeOf(v).Kind() == reflect.Slice {
				fmt.Println("Type is a Slice")
				fmt.Println("Length is: ", len(v.([]interface{})))

				for idx, value := range v.([]interface{}) {
					fmt.Println(idx, value)
					innerValue := value.(map[string]interface{})
					fmt.Println(innerValue["id"])
				}
			}
		}
	}
}

// Queries a single path in a json message. It returns an interface because the caller
// understands the context and will need to cast it to the appropriate type.
// This can be used to search for a specific value at a path, or to return a subtree that
// can be parsed further. E.g. Return a float of an array.
func queryPath(input *map[string]interface{}, queryString string) interface{} {
	var resp interface{}

	query, err := gojq.Parse(queryString)
	if err != nil {
		log.Fatal(err)
	}

	iter := query.Run(*input) // or query.RunWithContext

	// While there are more items to fetch - fetch them.
	for value, more := iter.Next(); more; value, more = iter.Next() {
		// "more" and "value" are dual-purpose result codes. In the gojq code the bool result is the
		// inverse of "done". So result is false when done and true when not done. If true, the
		// Interface{} result can be cast to a value or an Error type. So if true you should check
		// if it was an error before checking for the result.
		if err, more := value.(error); more {
			log.Fatalln(err)
		} else if value == nil {
			var e error = fmt.Errorf("query returns no result: <%s>", queryString)
			log.Fatal(e)
		} else {
			resp = value
			fmt.Println(reflect.TypeOf(resp))
		}
	}

	return resp
}

// queryDimensions searches within the dimensions hierachy for specified key/values.
// It returns an interface because the result can be more than one type. The caller
// is responsible for knowing how to convert the type.
func queryDimensions(input *map[string]interface{}, key string) interface{} {
	var resp interface{}

	var v interface{}
	v = queryPath(input, ".structure.dimensions.series")
	fmt.Println(reflect.TypeOf(v))

	// This path should return an array of dimensions. After retrieving the array
	// we need to extract the nominated value.
	if reflect.TypeOf(v).Kind() == reflect.Slice {
		// Cast the object
		m := v.([]interface{})
		log.Println("number of observations: ", len(m))
		log.Println("preview of the observations: ", gojq.Preview(m))

		extractDimensionData(m, key)
	}

	resp = v

	return resp
}

// Scan the array of dimensions for the nominated key and return the value
// Returns nil if the key could not be found.
// TODO: Add error handling
func extractDimensionData(input []interface{}, key string) interface{} {
	var resp interface{} = nil

	for _, value := range input {
		innerValue := value.(map[string]interface{})
		if key == innerValue["id"] {
			// Cast the "values" object and extract the value
			// values is a single-element array. So need to cast twice to an array of interfaces
			// index the array and cast the result to a map[string]interface{}. Argh!
			values := innerValue["values"].([]interface{})[0].(map[string]interface{})
			resp = values["id"]
			log.Println(innerValue["id"], ": ", resp)

			break
		}
	}

	return resp
}
