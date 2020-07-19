package util

import (
	"log"
	"reflect"
)

//InnerJoin takes an array of arrays of strings, inner-joins them,
//and returns the resulting array of strings
func InnerJoin(data [][]string) []string {
	//trivial case
	if len(data) == 1 {
		return data[0]
	}

	//as this is an inner join, every element that is in the final array will be in each and every input array
	//We will create a base, and compare each element in `base` with `rest`
	base := data[0]
	rest := data[1:]

	var result []string

	for _, s := range base {
		//Count the number of hits. If it equals the number of arrays in rest, we have a hit in the inner-join.
		count := 0
		for _, array := range rest {
			for _, item := range array {
				if item == s {
					count++
					break
				}
			}
			if count == len(rest) {
				result = append(result, s)
			}
		}
	}
	return result
}

//FlattenJSON takes a json object and flattens it.
//Going from
// {
//     "name": "Jo",
//     "lastname": "Walker",
//     "age": 53,
//     "brother": {
//         "name": "Simon",
//         "age": 55,
//         "bike": "VTT"
//     }
// }
// to
//
// {
//     "name": "Jo",
//     "lastname": "Walker",
//     "age": 53,
//     "brother.name": "Simon",
//     "brother.age": 55,
//     "brother.bike": "VTT"
// }
func FlattenJSON(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	log.Printf("Flatten JSON with %v", data)
	for k, v := range data {
		if IsJSONObj(k, data) {
			flattened := flattenRec(k, v.(map[string]interface{}))
			for kP, vP := range flattened {
				result[kP] = vP
			}
			delete(result, k)
		} else {
			result[k] = v
		}

	}
	return result
}

//flattenRec loops through items in data, adding to a root map, and prepending `key`
//to each entry name
func flattenRec(key string, data map[string]interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})
	//finished := true
	for k, v := range data {
		prefix := key + "." + k
		if IsJSONObj(k, data) {
			for kP, vP := range flattenRec(prefix, v.(map[string]interface{})) {
				flattened[kP] = vP
			}
			//finished = false
		} else {
			flattened[prefix] = v
		}

	}
	return flattened
}

//IsJSONObj returns true if element at `data[k]` is a json object
func IsJSONObj(k string, data map[string]interface{}) bool {
	obj := make(map[string]interface{})
	obj["hey"] = "yo"
	return reflect.TypeOf(data[k]) == reflect.TypeOf(obj)
}
