package util

import (
	"encoding/json"
	"log"
	"nosql-db/pkg/datatypes"
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

//GetJSON object from string
func GetJSON(data string) datatypes.JS {
	var dat map[string]interface{}
	log.Print(data)
	if err := json.Unmarshal([]byte(data), &dat); err != nil {
		//TODO handle this more graciously. Namely, check if it is a
		//JSON formatting issue, and return error to user.
		//UPDATE: this will definitely be needed if, when deleting elements from the db,
		//this is only done in the index file, and not the attribute file. In that case,
		//lookups in the attribute file will give hits to IDs which no longer exist in the
		//index file.
		panic(err)
	}
	return ConvertToJSON(dat)
}

//ConvertToJSON recursively descends a map[string]interface{}, converting all
//inner data into JS structs
func ConvertToJSON(data map[string]interface{}) datatypes.JS {
	js, _ := convertToJSON(data).(datatypes.JS)
	return js
}

func convertToJSON(data interface{}) interface{} {
	if !isJSPrimitive(data) {
		return data
	}
	dataObj := data.(map[string]interface{})
	jsObj := datatypes.JS{}
	for k, v := range dataObj {
		jsObj[k] = convertToJSON(v)
	}
	return jsObj
}

func IsObj(data interface{}) bool {
	_, ok := data.(datatypes.JS)
	return ok
}

func isJSPrimitive(data interface{}) bool {
	_, ok := data.(map[string]interface{})
	return ok
}

func mergeRFC7396(target, patch interface{}) interface{} {
	if IsObj(patch) {
		patchObj, _ := patch.(datatypes.JS)
		var targetObj datatypes.JS
		if !IsObj(target) {
			targetObj = datatypes.JS{} // Ignore the contents and set it to an empty Object
		} else {
			targetObj = target.(datatypes.JS)
		}
		for k, v := range patchObj {
			if v == nil {
				delete(targetObj, k)
			} else {
				targetObj[k] = mergeRFC7396(targetObj[k], v)
			}
		}
		return targetObj
	}
	return patch
}

//MergeRFC7396 implements RFC7396 to patch a json object according to another
//From https://tools.ietf.org/html/rfc7396
// Given the following example JSON document:
//
//    {
//      "title": "Goodbye!",
//      "author" : {
//        "givenName" : "John",
//        "familyName" : "Doe"
//      },
//      "tags":[ "example", "sample" ],
//      "content": "This will be unchanged"
//    }
//
// And the following PATCH object
//    {
//	 	"title": "Hello!",
//	 	"phoneNumber": "+01-123-456-7890",
//	 	"author": {
//	 	  "familyName": null
//	 	},
//	 	"tags": [ "example" ]
//   }
//
//   The resulting JSON document would be:
//   {
//	 	"title": "Hello!",
//	 	"author" : {
//	 	  "givenName" : "John"
//	 	},
//	 	"tags": [ "example" ],
//	 	"content": "This will be unchanged",
//	 	"phoneNumber": "+01-123-456-7890"
//   }
func MergeRFC7396(target, patch datatypes.JS) datatypes.JS {
	result := mergeRFC7396(target, patch)
	log.Printf("Merge result is %v", result)
	return result.(datatypes.JS)
	//return mergeRFC7396(inPrimitiveFormTarget, inPrimitiveFormPatch).(datatypes.JS)
}

//UniqueIDs returns the input list without any duplicates
func UniqueIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	j := 0
	for _, v := range ids {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		ids[j] = v
		j++
	}
	return ids[:j]
}
