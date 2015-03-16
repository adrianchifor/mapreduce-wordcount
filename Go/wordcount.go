package main

import (
    "iterator"
    "fmt"
    "mapreduce"
    "os"
    "regexp"
)

func find_files(dirname string) chan interface{} {
    output := make(chan interface{})

    go func() {
        _find_files(dirname, output)
        close(output)
    }()

    return output
}

func _find_files(dirname string, output chan interface{}) {
    dir, _ := os.Open(dirname)
    dirnames, _ := dir.Readdirnames(-1)

    for i := 0; i < len(dirnames); i++ {
        fullpath := dirname + "/" + dirnames[i]
        file, _ := os.Stat(fullpath)

        if file.IsDir() {
            _find_files(fullpath, output)
        } else {
            output <- fullpath
        }
    }
}

func word_count(filename interface{}, output chan interface{}) {
    results := map[string]int{}
    words := regexp.MustCompile(`[A-Za-z0-9_]*`)

    for line := range iterator.line(filename.(string)) {
        for _, match := range words.FindAllString(line, -1) {
            results[match]++
        }
    }

    output <- results
}

func reducer(input chan interface{}, output chan interface{}) {
    results := map[string]int{}

    for new_matches := range input {
        for key, value := range new_matches.(map[string]int) {
            previous_count, exists := results[key]

            if !exists {
                results[key] = value
            } else {
                results[key] = previous_count + value
            }
        }
    }

    output <- results
}

func main() {
    fmt.Print(mapreduce.MapReduce(word_count, reducer, find_files("."), 20))
}
