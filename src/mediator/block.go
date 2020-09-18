package mediator

// blocks' information help save file and keep load balance 4 node server
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

var defaultBlockSize int = 64 * 1024 * 1024

type BlockList []*Block

// implements sort interface
func (this BlockList) Len() int {
	return len(this)
}

func (this BlockList) Less(i, j int) bool {
	rate1 := this[i].getFillingRate()
	rate2 := this[j].getFillingRate()

	return rate1 < rate2
}

func (this BlockList) Swap(i, j int) {
	var temp *Block = this[i]
	this[i] = this[j]
	this[j] = temp
}

func (this *Block) getFillingRate() int {
	return this.End / this.Size
}
