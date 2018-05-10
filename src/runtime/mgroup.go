package runtime

import (
	"runtime/internal/atomic"
	"unsafe"
)

const (
	mgroupactive uint32 = iota
	mgroupinactive
)

func mgroupwaitforg() (gp *g, inheritTime bool) {
	_m_ := getg().m
runmgroup:
	// var gp *g

	// println("# Thread", _m_.id, "looking for work")

	// Check local M queues
	lock(&_m_.mgrouplock)
	gp, inheritTime = mgrouprunqget(_m_)
	unlock(&_m_.mgrouplock)
	if gp != nil {
		// println("# Thread", _m_.id, "running goid ", gp.goid)

		status := readgstatus(gp)
		if status&^_Gscan != _Grunnable {
			println("# Thread", _m_.id, "goid", gp.goid, " is not Grunnable or Gscanrunnable")
			dumpgstatus(gp)
			breakpoint()
			throw("# not runnable")
		}

		return
	}

	// println("# Thread", _m_.id, "sleeping with P", _m_.p.ptr().id)
	// println("")

	// Schedule another M to run this p.
	atomic.Cas(&_m_.mgroupstatus, mgroupactive, mgroupinactive)

	// lock(&_m_.mgrouplock)
	// if !mgrouprunqempty(_m_) {
	// 	println("# Thread", _m_.id, "aborted sleep")
	// 	unlock(&_m_.mgrouplock)
	// 	goto runmgroup
	// }

	_p_ := releasep()
	handoffp(_p_)
	incidlelocked(1)
	// unlock(&_m_.mgrouplock)

	notesleep(&_m_.park)
	// var busy int64
	// for _m_.mgroupstatus != mgroupactive {
	// 	busy++
	// 	KeepAlive(busy)
	// }

	// for _m_.nextp != 0 {
	// 	busy++
	// 	KeepAlive(busy)
	// }
	// println("# Thread", _m_.id, "awoke with nextp =", _m_.nextp.ptr())

	// lock(&_m_.mgrouplock)
	noteclear(&_m_.park)
	acquirep(_m_.nextp.ptr())
	_m_.nextp = 0
	// unlock(&_m_.mgrouplock)

	// println("# Thread", _m_.id, "awoke with P", _m_.p.ptr().id)

	// if mgrouprunqempty(_m_) {
	// 	// throw("Thread woken up, but runq is empty")
	// 	goto runmgroup
	// }

	if _m_.mgroupcount == 0 {
		gp, inheritTime = nil, false
		return
	}

	goto runmgroup
}

func startmgroupg(gp *g) {
	_g_ := getg()
	status := readgstatus(gp)
	if status&^_Gscan != _Grunnable {
		println("# [Transfer] Thread", _g_.m.id, "goid", gp.goid, " is not Grunnable or Gscanrunnable")
		print("# [Transfer] g is not Grunnable or Gscanrunnable\n")
		dumpgstatus(_g_)
		throw("# Scheduler:  not runnable")
	}

	mp := gp.mgroup.ptr()
	// println("# [Transfer] Thread", _g_.m.id, "adding goid", gp.goid, "to thread", mp.id)

	lock(&mp.mgrouplock)
	mgrouprunqput(mp, gp, false)
	unlock(&mp.mgrouplock)

	if atomic.Cas(&mp.mgroupstatus, mgroupinactive, mgroupactive) {
		// directly handoff current P to the locked m
		incidlelocked(-1)
		_p_ := releasep()
		// println("# [Transfer] Waking thread", mp.id, "with P", _p_.id)
		// println("")
		mp.nextp.set(_p_)
		notewakeup(&mp.park)

		// unlock(&mp.mgrouplock)

		stopm()
	}

	// if mp.p == 0 && mp.nextp == 0 {
	// 	// directly handoff current P to the locked m
	// 	incidlelocked(-1)
	// 	_p_ := releasep()
	// 	// println("# [Transfer] Waking thread", mp.id, "with P", _p_.id)
	// 	// println("")
	// 	mp.nextp.set(_p_)
	// 	notewakeup(&mp.park)
	// 	unlock(&mp.mgrouplock)

	// 	stopm()
	// } else {
	// 	// M already has a P
	// 	// println("# [Transfer] Thread", mp.id, "already has P")
	// 	// println("")
	// 	unlock(&mp.mgrouplock)
	// }
}

////////////////////////////

func mgrouprunqprint(_m_ *m) {
	h := atomic.Load(&_m_.runqhead) // load-acquire, synchronize with consumers
	t := _m_.runqtail
	size := t - h

	nextgor := _m_.runnext.ptr()
	var nextgorid int64
	if nextgor != nil {
		nextgorid = _m_.runnext.ptr().goid
	}
	print("# [m=", _m_.id, "] RunQueue (size=", size, "|h=", h, "|t=", t, "|next=(", nextgor, ") ", nextgorid, "):")
	for ; h != t; h++ {
		// h = h % uint32(len(_m_.runq))
		gp := _m_.runq[h%uint32(len(_m_.runq))].ptr()
		// if size == 0 {
		// 	println("")
		// 	println("Error - Runqueue size does not match linked list")
		// 	println("Wanted to print: gp =", gp)
		// 	breakpoint()
		// }
		print(" -> (", gp, ") ", gp.goid)
		size--
	}
	println("")
}

func mgrouprunqempty(_m_ *m) bool {
	// Defend against a race where 1) _p_ has G1 in runqnext but runqhead == runqtail,
	// 2) runqput on _p_ kicks G1 to the runq, 3) runqget on _p_ empties runqnext.
	// Simply observing that runqhead == runqtail and then observing that runqnext == nil
	// does not mean the queue is empty.
	for {
		head := atomic.Load(&_m_.runqhead)
		tail := atomic.Load(&_m_.runqtail)
		runnext := atomic.Loaduintptr((*uintptr)(unsafe.Pointer(&_m_.runnext)))
		if tail == atomic.Load(&_m_.runqtail) {
			return head == tail && runnext == 0
		}
	}
}

func mgrouprunqput(_m_ *m, gp *g, next bool) {
	if randomizeScheduler && next && fastrand()%2 == 0 {
		next = false
	}

	// mgrouprunqprint(_m_)

	if next {
	retryNext:
		oldnext := _m_.runnext
		if !_m_.runnext.cas(oldnext, guintptr(unsafe.Pointer(gp))) {
			goto retryNext
		}
		if oldnext == 0 {
			return
		}
		// Kick the old runnext out to the regular run queue.
		gp = oldnext.ptr()
	}

	// retry:
	h := atomic.Load(&_m_.runqhead) // load-acquire, synchronize with consumers
	t := _m_.runqtail
	if t-h < uint32(len(_m_.runq)) {
		_m_.runq[t%uint32(len(_m_.runq))].set(gp)
		atomic.Store(&_m_.runqtail, t+1) // store-release, makes the item available for consumption
		// mgrouprunqprint(_m_)
		return
	}

	println("MGroup Queue is full")
	// add to global runq instead
	lock(&sched.lock)
	globrunqput(gp)
	unlock(&sched.lock)

	// if runqputslow(_p_, gp, h, t) {
	// 	return
	// }

	// the queue is not full, now the put above must succeed
	// goto retry
}

// func mgrouprunqputslow(_p_ *p, gp *g, h, t uint32) bool {
// 	var batch [len(_p_.runq)/2 + 1]*g

// 	// First, grab a batch from local queue.
// 	n := t - h
// 	n = n / 2
// 	if n != uint32(len(_p_.runq)/2) {
// 		throw("runqputslow: queue is not full")
// 	}
// 	for i := uint32(0); i < n; i++ {
// 		batch[i] = _p_.runq[(h+i)%uint32(len(_p_.runq))].ptr()
// 	}
// 	if !atomic.Cas(&_p_.runqhead, h, h+n) { // cas-release, commits consume
// 		return false
// 	}
// 	batch[n] = gp

// 	if randomizeScheduler {
// 		for i := uint32(1); i <= n; i++ {
// 			j := fastrandn(i + 1)
// 			batch[i], batch[j] = batch[j], batch[i]
// 		}
// 	}

// 	// Link the goroutines.
// 	for i := uint32(0); i < n; i++ {
// 		batch[i].schedlink.set(batch[i+1])
// 	}

// 	// Now put the batch on global queue.
// 	lock(&sched.lock)
// 	globrunqputbatch(batch[0], batch[n], int32(n+1))
// 	unlock(&sched.lock)
// 	return true
// }

func mgrouprunqget(_m_ *m) (gp *g, inheritTime bool) {
	// mgrouprunqprint(_m_)

	// If there's a runnext, it's the next G to run.
	for {
		next := _m_.runnext
		if next == 0 {
			break
		}
		if _m_.runnext.cas(next, 0) {
			return next.ptr(), true
		}
	}

	for {
		h := atomic.Load(&_m_.runqhead) // load-acquire, synchronize with other consumers
		t := _m_.runqtail
		if t == h {
			return nil, false
		}
		gp := _m_.runq[h%uint32(len(_m_.runq))].ptr()
		if atomic.Cas(&_m_.runqhead, h, h+1) { // cas-release, commits consume
			return gp, false
		}
	}
}

///////////////////////////////////////

//go:nosplit
func GetGoID() int64 {
	_g_ := getg()
	return _g_.goid
}

//go:nosplit
func GetMID() int64 {
	_g_ := getg()
	return _g_.m.id
}

//go:nosplit
func GetPID() int32 {
	_g_ := getg()
	return _g_.m.p.ptr().id
}

//go:nosplit
func CreateMGroup() (mgid int64) {
	if atomic.Load(&newmHandoff.haveTemplateThread) == 0 && GOOS != "plan9" {
		// If we need to start a new thread from the locked
		// thread, we need the template thread. Start it now
		// while we're in a known-good state.
		startTemplateThread()
	}

	if atomic.Load(&sched.npidle) != 0 && atomic.Load(&sched.nmspinning) == 0 {
		wakep()
	}

	_g_ := getg()

	// sanity check - can be removed
	if _g_ != _g_.m.curg {
		throw("We have migrared to system stack!")
	}

	atomic.Xadd(&_g_.m.mgroupcount, 1)
	_g_.mgroup.set(_g_.m)
	mgid = _g_.m.id
	return
}

//go:nosplit
func GetMGroup() (mgid int64) {
	_g_ := getg()

	// sanity check - can be removed
	if _g_ != _g_.m.curg {
		throw("We have migrared to system stack!")
	}

	if _g_.mgroup == 0 {
		return -1
	}

	mgid = _g_.mgroup.ptr().id
	return
}

//go:nosplit
func JoinMGroup(mgid int64) (ok bool) {
	_g_ := getg()

	// sanity check - can be removed
	if _g_ != _g_.m.curg {
		throw("We have migrared to system stack!")
	}

	// //TODO: add code to detect chages and decrement lockedExt
	_m_ := allm
	for {
		if _m_.id == mgid {
			if _m_.mgroupcount == 0 {
				throw("Tried to join an MGroup that has not been created")
			}
			atomic.Xadd(&_m_.mgroupcount, 1)
			_g_.mgroup.set(_m_)
			ok = true
			return
		}
		_m_ = _m_.alllink
		if _m_ == allm {
			break
		}
	}

	ok = false
	return
}

//go:nosplit
func LeaveMGroup() {
	_g_ := getg()

	// sanity check - can be removed
	if _g_ != _g_.m.curg {
		throw("We have migrared to system stack!")
	}

	if _g_.mgroup == 0 {
		throw("Called LeaveMGroup without being part of an MGroup")
	}

	if _g_.mgroup.ptr().mgroupcount == 0 {
		throw("Called LeaveMGroup, but mgroup count is already 0")
	}

	atomic.Xadd(&_g_.mgroup.ptr().mgroupcount, -1)
	_g_.mgroup = 0
}
