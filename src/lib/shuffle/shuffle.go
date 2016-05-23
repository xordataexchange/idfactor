package shuffle

import (
	"crypto/rand"
	"log"
	"math/big"
)

// Shuffle returns an unpredictable permuation of the integers [0,n)
func Shuffle(n int) []int {
	if n < 1 {
		log.Fatal("idfac/shuffle: n must be positive in call to Shuffle")
	}
	a := make([]int, n)
	for i := 0; i != n; i++ {
		J, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			log.Fatalf("idfac/shuffle: failed to generate random number in call to Shuffle: %s", err)
		}
		j := J.Int64()
		a[i], a[j] = a[j], i
	}
	return a
}
