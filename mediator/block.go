package mediator





// blocks' information help save file and keep load balance 4 node server
//
// 块信息有助于保存文件和保持负载平衡。
type Block struct {
	BlockId int
	DataId  int    // all this block's records save in one center data
	Dir     string // host disk dir
	Addr    string // host net address
	Size    int    // block size
	End     int    // records offset sum
}

/*
	End       int    // not accurate, refresh by mediator
	OidNumber int    // this block contains oid number
	VisitAvg  int
*/

// 64 MB
var defaultBlockSize = 64 * 1024 * 1024

// 块列表
type BlockList []*Block


// implements sort interface
func (bl BlockList) Len() int {
	return len(bl)
}

func (bl BlockList) Less(i, j int) bool {
	rate1 := bl[i].getFillingRate()
	rate2 := bl[j].getFillingRate()

	return rate1 < rate2
}

func (bl BlockList) Swap(i, j int) {
	var temp *Block = bl[i]
	bl[i] = bl[j]
	bl[j] = temp
}

func (b *Block) getFillingRate() int {
	return b.End / b.Size
}
