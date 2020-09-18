package center

import (
	"common"
)

func AddHandler2CenterServer(this *CenterServer) {
	this.Handlers = []*CenterServerHandler{}

	// *** close
	h := &CenterServerHandler{Command: CMD_CLOSE}
	h.Fn = func(p PackRecord) PackRecord {
		this.Close()

		r := PackRecord{}
		r.Flag = true
		return r
	}
	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ",
		len(this.Handlers))

	// *** put record
	h = &CenterServerHandler{Command: CMD_PUT_RECORD}
	h.Fn = func(p PackRecord) PackRecord {
		r := PackRecord{}

		rec := p.Rec
		oidInfo := GetOidInfo(rec.Oid)
		e := this.Center.Set(oidInfo.DataId, rec)
		if e != nil {
			r.Flag = false
			r.Msg = "center set error - " + e.Error()
		} else {
			r.Flag = true
		}
		return r
	}
	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ",
		len(this.Handlers))

	// *** get oid meta
	h = &CenterServerHandler{Command: CMD_GET_OID_META}
	h.Fn = func(p PackRecord) PackRecord {
		r := PackRecord{}

		oid := p.Oid
		oidInfo := GetOidInfo(oid)
		rec, e := this.Center.Get(oidInfo.DataId, oid)
		if e != nil {
			r.Flag = false
			r.Msg = "center get error - " + e.Error()
		} else {
			r.Rec = rec
			r.Flag = true
		}
		return r
	}
	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ",
		len(this.Handlers))

	// *** change status
	h = &CenterServerHandler{Command: CMD_CHANGE_OID_STATUS}
	h.Fn = func(p PackRecord) PackRecord {
		r := PackRecord{}

		oid := p.Oid
		oidInfo := GetOidInfo(oid)
		rec, e := this.Center.Get(oidInfo.DataId, oid)
		if e != nil {
			r.Flag = false
			r.Msg = "center update status disable get error - " + e.Error()
		} else {
			rec.Status = p.Status
			e := this.Center.Set(oidInfo.DataId, rec)
			if e != nil {
				r.Flag = false
				r.Msg = "center update status disable set error - " + e.Error()
			} else {
				// status next process queue TODO
				r.Flag = true
			}
		}
		return r
	}
	this.Handlers = append(this.Handlers, h)
	common.Log.Info("center server handler added - "+h.Command+" and handlers' len is ",
		len(this.Handlers))
}
