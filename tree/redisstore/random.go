package redisstore

import (
	"math/rand"
	"sync"
	"time"
)

// Code below is an adaptation of https://github.com/nishanths/go-xkcd/blob/b5a58daa228c66d55ead5da14125567329173ca6/random.go

type lockedRandSource struct {
	lock sync.Mutex
	src  rand.Source
}

// rnd is a source of random numbers safe for concurrent use by multiple goroutines
// used to generate task lock values
var rnd *rand.Rand

func init() {
	rnd = rand.New(&lockedRandSource{src: rand.NewSource(time.Now().UnixNano())})
}

// to satisfy rand.Source interface
func (r *lockedRandSource) Int63() int64 {
	r.lock.Lock()
	ret := r.src.Int63()
	r.lock.Unlock()
	return ret
}

// to satisfy rand.Source interface
func (r *lockedRandSource) Seed(seed int64) {
	r.lock.Lock()
	r.src.Seed(seed)
	r.lock.Unlock()
}

func randString(n int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	str := make([]byte, n)
	for i := range str {
		str[i] = chars[rand.Intn(len(chars))]
	}
	return string(str)
}
