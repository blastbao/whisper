package center

import (
	"common"
	"fmt"
	"testing"
)

func TestGetOidInfo(t *testing.T) {
	oid := GenOid(1, 2)
	common.Log.Info("oid info", GetOidInfo(oid))
	common.Log.Info("oid siblings", GetOidSiblings(oid))
}

func LoopGenOid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenOid(1, 1)
	}
}

func TestGenOid(t *testing.T) {
	br := testing.Benchmark(LoopGenOid)
	fmt.Println(br)
}
