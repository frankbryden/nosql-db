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
	var dat datatypes.JS
	if err := json.Unmarshal([]byte(data), &dat); err != nil {
		//TODO handle this more graciously. Namely, check if it is a
		//JSON formatting issue, and return error to user.
		//UPDATE: this will definitely be needed if, when deleting elements from the db,
		//this is only done in the index file, and not the attribute file. In that case,
		//lookups in the attribute file will give hits to IDs which no longer exist in the
		//index file.
		panic(err)
	}
	return dat
}

func IsJSONObj2(data interface{}) bool {
	_, ok := data.(datatypes.JS)
	return ok
}

func mergeRFC7396(target, patch interface{}) interface{} {
	log.Printf("%v, %v", target, patch)
	if IsJSONObj2(patch) {
		patchObj := patch.(datatypes.JS)
		var targetObj datatypes.JS
		if !IsJSONObj2(target) {
			targetObj = datatypes.JS{} // Ignore the contents and set it to an empty Object
		} else {
			log.Printf("Here with %v", target)
			targetObj = target.(datatypes.JS)
		}
		for k, v := range patchObj {
			log.Printf("%v -> %v", k, v)
			if v == nil {
				delete(targetObj, k)
			} else {
				targetObj[k] = mergeRFC7396(targetObj[k], v)
			}
		}
		return targetObj
	} else {
		log.Printf("%v is not an object", patch)
		log.Printf("Type : %v", reflect.TypeOf(patch))
		_, ok := patch.(datatypes.JS)
		log.Printf("On first test: %t, On second test: %t", IsJSONObj2(patch), ok)
		return patch
	}

}

func MergeRFC7396(target, patch interface{}) datatypes.JS {
	return mergeRFC7396(target, patch).(datatypes.JS)
}
