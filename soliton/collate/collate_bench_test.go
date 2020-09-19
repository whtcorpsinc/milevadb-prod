package defCauslate

import (
	"math/rand"
	"testing"

	_ "net/http/pprof"
)

const short = 2 << 4
const midbse = 2 << 10
const long = 2 << 20

func generateData(length int) string {
	rs := []rune("ßss")
	r := make([]rune, length)
	for i := range r {
		r[i] = rs[rand.Intn(len(rs))]
	}

	return string(r)
}

func compare(b *testing.B, defCauslator DefCauslator, length int) {
	s1 := generateData(length)
	s2 := generateData(length)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if defCauslator.Compare(s1, s2) != 0 {
		}
	}
}

func key(b *testing.B, defCauslator DefCauslator, length int) {
	s := generateData(length)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		defCauslator.Key(s)
	}
}

func BenchmarkUtf8mb4Bin_CompareShort(b *testing.B) {
	compare(b, &binDefCauslator{}, short)
}

func BenchmarkUtf8mb4GeneralCI_CompareShort(b *testing.B) {
	compare(b, &generalCIDefCauslator{}, short)
}

func BenchmarkUtf8mb4UnicodeCI_CompareShort(b *testing.B) {
	compare(b, &unicodeCIDefCauslator{}, short)
}

func BenchmarkUtf8mb4Bin_CompareMid(b *testing.B) {
	compare(b, &binDefCauslator{}, midbse)
}

func BenchmarkUtf8mb4GeneralCI_CompareMid(b *testing.B) {
	compare(b, &generalCIDefCauslator{}, midbse)
}

func BenchmarkUtf8mb4UnicodeCI_CompareMid(b *testing.B) {
	compare(b, &unicodeCIDefCauslator{}, midbse)
}

func BenchmarkUtf8mb4Bin_CompareLong(b *testing.B) {
	compare(b, &binDefCauslator{}, long)
}

func BenchmarkUtf8mb4GeneralCI_CompareLong(b *testing.B) {
	compare(b, &generalCIDefCauslator{}, long)
}

func BenchmarkUtf8mb4UnicodeCI_CompareLong(b *testing.B) {
	compare(b, &unicodeCIDefCauslator{}, long)
}

func BenchmarkUtf8mb4Bin_KeyShort(b *testing.B) {
	key(b, &binDefCauslator{}, short)
}

func BenchmarkUtf8mb4GeneralCI_KeyShort(b *testing.B) {
	key(b, &generalCIDefCauslator{}, short)
}

func BenchmarkUtf8mb4UnicodeCI_KeyShort(b *testing.B) {
	key(b, &unicodeCIDefCauslator{}, short)
}

func BenchmarkUtf8mb4Bin_KeyMid(b *testing.B) {
	key(b, &binDefCauslator{}, midbse)
}

func BenchmarkUtf8mb4GeneralCI_KeyMid(b *testing.B) {
	key(b, &generalCIDefCauslator{}, midbse)
}

func BenchmarkUtf8mb4UnicodeCI_KeyMid(b *testing.B) {
	key(b, &unicodeCIDefCauslator{}, midbse)
}

func BenchmarkUtf8mb4Bin_KeyLong(b *testing.B) {
	key(b, &binDefCauslator{}, long)
}

func BenchmarkUtf8mb4GeneralCI_KeyLong(b *testing.B) {
	key(b, &generalCIDefCauslator{}, long)
}

func BenchmarkUtf8mb4UnicodeCI_KeyLong(b *testing.B) {
	key(b, &unicodeCIDefCauslator{}, long)
}
