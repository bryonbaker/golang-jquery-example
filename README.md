# Golang JSON message Parser GoJq Example

This example illustrates how to use itchny/gojq to query a complex JOSN message.

The message is from the European Central Bank's FS Market Pricing service: https://sdw-wsrest.ecb.europa.eu/help/

The output from tyhis code is:
```
Running JSON Query: .header.sender.id
"ECB"
Type: string

Running JSON Query: .dataSets[0].series."0:0:0:0:0".observations."0"[0]
1.4529
Type: float64

Running JSON Query: .dataSets[0].series."0:0:0:0:0".observations
map[string]interface {}{"0":[]interface {}{1.4529, 0, 0, interface {}(nil), interface {}(nil)}, "1":[]interface {}{1.4472, 0, 0, interface {}(nil), interface {}(nil)}, "2":[]interface {}{1.4591, 0, 0, interface {}(nil), interface {}(nil)}, "3":[]interface {}{1.4651, 0, 0, interface {}(nil), interface {}(nil)}, "4":[]interface {}{1.4671, 0, 0, interface {}(nil), interface {}(nil)}}
Type: map[string]interface {}
Number of observations:  5
Preview of the observations: {"0":[1.4529,0,0,null,nul ...}
Observation 0: [1.4529 0 0 <nil> <nil>]
Observation type: []interface {}
1.4529
float64
```