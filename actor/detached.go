package actor

type detachedRcvr struct {
	localRcvr
	h DetachedHandler
}

func (r *detachedRcvr) start() {
	go r.h.Start(&r.ctx)
	r.localRcvr.start()
}

func (r *detachedRcvr) handleMsg(mh msgAndHandler) {
	r.h.Recv(mh.msg, &r.ctx)
}

func (r *detachedRcvr) handleCmd(cmd routineCmd) {
	switch {
	case cmd.cmdType == stopRoutine:
		r.h.Stop(&r.ctx)
		r.localRcvr.handleCmd(cmd)
	}
}
