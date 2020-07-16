package util

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
