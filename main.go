package main

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"

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

	testQueries := []string{".header.sender.id",
		".dataSets[0].series.\"0:0:0:0:0\".observations.\"0\"[0]",
		".dataSets[0].series.\"0:0:0:0:0\".observations"}

	for _, v := range testQueries {
		runQuery(&input, v)
	}
}

func runQuery(input *map[string]interface{}, queryString string) {
	fmt.Printf("\nRunning JSON Query: %s\n", queryString)

	query, err := gojq.Parse(queryString)

	if err != nil {
		log.Fatalln(err)
	}

	iter := query.Run(*input) // or query.RunWithContext

	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			log.Fatalln(err)
		}
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
			fmt.Println(reflect.TypeOf(val))
		}
	}
}
