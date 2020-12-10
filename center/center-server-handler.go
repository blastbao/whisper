package center

import (
	"github.com/blastbao/whisper/common"
)




// 为 CenterServer 添加 Handlers ，用于处理接收到的请求。
//
// CMD_CLOSE: 关闭 CenterServer
// CMD_PUT_RECORD: 根据 indexId 查询 index ，然后把新 record 保存到 index 中。
// CMD_GET_OID_META: 根据 indexId 查询 index ，然后从 index 中取出 oid 对应的 record 。
// CMD_CHANGE_OID_STATUS: 根据 indexId 查询 index ，然后更新其中 record 的 status。
//
func AddHandler2CenterServer(this *CenterServer) {

	this.Handlers = []*CenterServerHandler{}

	// *** close
	h := &CenterServerHandler{
		Command: CMD_CLOSE,
		Fn: func(p PackRecord) PackRecord {
			// 关闭 CenterServer
			this.Close()
			// 构造回包
			r := PackRecord{}
			r.Flag = true
			return r
		},
	}

	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ", len(this.Handlers))

	// *** put record
	h = &CenterServerHandler{
		Command: CMD_PUT_RECORD,
		Fn: func(p PackRecord) PackRecord {
			r := PackRecord{}
			rec := p.Rec
			oidInfo := GetOidInfo(rec.Oid)
			// 根据 indexId 查询 index ，然后把新 record 保存到 index 中。
			e := this.Center.Set(oidInfo.IndexId, rec)
			if e != nil {
				r.Flag = false
				r.Msg = "center set error - " + e.Error()
			} else {
				r.Flag = true
			}
			return r
		},
	}

	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ", len(this.Handlers))

	// *** get oid meta
	h = &CenterServerHandler{
		Command: CMD_GET_OID_META,
		Fn: func(p PackRecord) PackRecord {
			r := PackRecord{}
			oid := p.Oid
			oidInfo := GetOidInfo(oid)
			// 根据 indexId 查询 index ，然后从 index 中取出 oid 对应的 record 。
			rec, e := this.Center.Get(oidInfo.IndexId, oid)
			if e != nil {
				r.Flag = false
				r.Msg = "center get error - " + e.Error()
			} else {
				// 设置返回值
				r.Rec = rec
				r.Flag = true
			}
			return r
		},
	}

	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ", len(this.Handlers))

	// *** change status
	h = &CenterServerHandler{
		Command: CMD_CHANGE_OID_STATUS,
		Fn: func(p PackRecord) PackRecord {
			r := PackRecord{}
			oid := p.Oid
			oidInfo := GetOidInfo(oid)
			// (1) 根据 indexId 查询 index ，然后从 index 中取出 oid 对应的 record 。
			rec, e := this.Center.Get(oidInfo.IndexId, oid)
			if e != nil {
				r.Flag = false
				r.Msg = "center update status disable get error - " + e.Error()
			} else {
				// (2) 更新 record 的 status
				rec.Status = p.Status
				// (3) 根据 indexId 查询 index ，然后把新 record 保存到 index 中。
				e := this.Center.Set(oidInfo.IndexId, rec)
				if e != nil {
					r.Flag = false
					r.Msg = "center update status disable set error - " + e.Error()
				} else {
					// status next process queue TODO
					r.Flag = true
				}
			}
			return r
		},
	}
	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ", len(this.Handlers))
}
