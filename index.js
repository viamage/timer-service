const r = require('rethinkdb')
const uuid = require('uuid')
const evs = require('rethink-event-sourcing')({
  serviceName: 'timer'
})

evs.onInstall(
  () => Promise.all([
    r.tableCreate('timers').run(evs.db)
  ])
)

let queueSize = 1024
let queueDuration = 10 * 60 * 1000

let timersQueue = [];
let timersById = new Map();
let timersLoopStarted = false
let timersLoopTimeout = 0

function fireTimer(timer) {
  runTimerAction(timer).catch(error => {
    let timerTimestamp = Date.now() + retryDelay
    let retries = timer.retries + 1
    if(retries > maxRetries) {
      evs.emitEvents("timer", [{
        type: "timerFinished",
        timerId: timer.id,
        error
      }], timer.sourceCommandId || 'timer')
      timersById.delete(timer.id)
    } else { // Retry
      evs.emitEvents("timer", [{
        type: "timerFailed",
        timerId: timer.id,
        error, timerTimestamp
      }], timer.sourceCommandId || 'timer')
      timer.timerTimestamp = timerTimestamp
      insertTimer(timer)
    }
  }).then(
    done => {
      timer.loops --
      if(timer.loops < 0) {
        evs.emitEvents("timer",[{
          type: "timerFinished",
          timerId : timer.id
        }], timer.sourceCommandId || 'timer')
        timersById.delete(timer.id)
      } else {
        let timerTimestamp = timer.timerTimestamp + timer.period
        evs.emitEvents("timer",[{
          type: "timerFired",
          timerId : timer.id,
          timerTimestamp
        }], timer.sourceCommandId || 'timer')
        timer.timerTimestamp = timerTimestamp
        insertTimer(timer)
      }
    }
  )
}

function timersLoop() {
  if(timersQueue.length == 0) {
    timersLoopStarted = false;
    return;
  }
  let nextTs = timersQueue[0].timerTimestamp
  let now = Date.now()
  while(nextTs < now) {
    fireTimer(timersQueue.shift())
    if(timersQueue.length == 0) {
      timersLoopStarted = false
      return
    }
    nextTs = timersQueue[0].timerTimestamp
  }
  let delay = nextTs - Date.now()
  if(delay > 1000) delay = 1000
  setTimeout(timersLoop, delay)
}

function startTimersLoop() {
  timersLoopStarted = true
  timersLoop()
}

function appendTimers(timers) {
  for(let timer of timers) {
    if(!timersById.has(timer.id)) {
      timersQueue.push(timer)
      timersById.set(timer.id, timer)
    }
  }
}

function maybeLoadMore() {
  let lastTime = timersQueue[timersQueue.length - 1].timerTimestamp
  if(lastTime - Date.now() < queueDuration) {
    r.table("timers").orderBy(r.asc("timerTimestamp")).filter(r=>r("timerTimestamp").ge(lastTime)).limit(1024).run(evs.db)
      .then(cursor => cursor.toArray())
      .then(timers => {
        appendTimers(timers)
        if(timers.length == queueSize) maybeLoadMore()
      })
  }
}

function startTimers() {
  r.table("timers").orderBy(r.asc("timerTimestamp")).limit(queueSize).run(evs.db)
    .then(cursor => cursor.toArray())
    .then(timers => {
      appendTimers(timers)
      if(timers.length == queueSize) maybeLoadMore()
      if(!timersLoopStarted) startTimersLoop()
    })
}

function runTimerAction(timer) {
  if(timer.command) return evs.call(timer.service, timer.command, "timer")
  return new Promise((resolve, reject) => resolve(true))
}

function insertTimer(timer) {
  timersById.set(timer.id, timer)
  for(let i = 0; i < timersQueue; i++) {
    if(timer.timerTimestamp < timersQueue[i].timerTimestamp) {
      timersQueue.splice(i, 0, timer)
      return;
    }
  }
  timersQueue.push(timer)
  if(!timersLoopStarted) {
    startTimersLoop()
  } else if(timer.timerTimestamp - Date.now() < 1000) {
    clearTimeout(timersLoopTimeout)
    startTimersLoop()
  }
}

function removeTimer(timerId) {
  for(let i = 0; i < timersQueue; i++) {
    if(timersQueue[i].id == timerId) {
      timersQueue.splice(i, 1)
    }
  }
  timersById.delete(timerId)
}

evs.onStart(startTimers)

evs.registerCommands({

  create(data, emit) {
    let { timerTimestamp, service, command } = data
    let loops = data.loops || 0
    let timerId = uuid.v4();
    let maxRetries = data.maxRetries || 0
    let retryDelay = data.retryDelay || 25 * 1000
    let period = data.period || 0
    if(loops > 0 && period ==0) throw evs.error("impossibleTimer")
    emit([{
      type: "timerCreated",
      timerTimestamp, service, command, loops, period, timerId, maxRetries, retryDelay
    }])
    insertTimer({ timerTimestamp, service, command, loops, period, id: timerId, sourceCommandId: data.id })
    return timerId
  },
  cancel({ timerId }, emit) {
    return r.table("timers").get(timerId).run(evs.db).then(
      timerRow => {
        if(!timerRow) throw evs.error("notFound")
        emit([{
          type: "timerCanceled", timerId
        }])
        removeTimer(timerId)
        return true
      }
    )
  }

})

evs.registerEventListeners({
  queuedBy: 'timerId',

  timerCreated({ timerId, timerTimestamp, service, command, loops, period, maxRetries, retryDelay, commandId }) {
    return r.table("timers").insert({
      id: timerId, timerTimestamp, service, command, loops, period, retries: 0, maxRetries, retryDelay,
      sourceCommandId: commandId
    }).run(evs.db)
  },
  timerCanceled({ timerId }) {
    return r.table("timers").get(timerId).delete().run(evs.db)
  },
  timerFinished({ timerId }) {
    return r.table("timers").get(timerId).delete().run(evs.db)
  },
  timerFired({ timerId, timerTimestamp }) {
    return r.table("timers").get(timerId).update(
      row => ({ loops: row("loops").sub(1), timerTimestamp })
    ).run(evs.db)
  },
  timerFailed({ timerId, timerTimestamp }) {
    return r.table("timers").get(timerId).update(
      row => ({ retries: row("retries").add(1), timerTimestamp })
    ).run(evs.db)
  }

})
