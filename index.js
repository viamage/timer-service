const r = require('rethinkdb')
const uuid = require('uuid')
const evs = require('rethink-event-sourcing')({
  serviceName: 'timer',
  noAutostart: true
})

evs.db = r.autoConnection()

let queueDuration = 1 * 60 * 1000
let loadMoreAfter = Math.floor(queueDuration / 2)

let timersQueue = [];
let timersById = new Map();
let timersLoopStarted = false
let timersLoopTimeout = 0
let lastLoadTime = 0

function fireTimer(timer) {
  runTimerAction(timer).catch(error => {
    console.error("TIMER ACTION ERROR", error)
    let timestamp = Date.now() + timer.retryDelay
    timer.retries ++
    if(timer.retries > timer.maxRetries) {
      evs.emitEvents("timer", [{
        type: "timerFinished",
        timer: timer.id,
        error
      }], { ...(timer.origin || {}), through: 'timer' })
      timersById.delete(timer.id)
    } else { // Retry
      evs.emitEvents("timer", [{
        type: "timerFailed",
        timer: timer.id,
        error, timestamp
      }], { ...(timer.origin || {}), through: 'timer' })
      timer.timestamp = timestamp
      insertTimer(timer)
    }
  }).then(
    done => {
      timer.loops --
      if(timer.loops < 0) {
        evs.emitEvents("timer",[{
          type: "timerFinished",
          timer: timer.id
        }], { ...(timer.origin || {}), through: 'timer' })
        timersById.delete(timer.id)
      } else {
        let timestamp = timer.timestamp + timer.interval
        evs.emitEvents("timer",[{
          type: "timerFired",
          timer: timer.id,
          timestamp
        }], { ...(timer.origin || {}), through: 'timer' })
        timer.timestamp = timestamp
        insertTimer(timer)
      }
    }
  )
}

async function timersLoop() {
  if(timersQueue.length == 0) {
    timersLoopStarted = false
    setTimeout(checkIfThereIsMore, loadMoreAfter)
    return
  }
  let nextTs = timersQueue[0].timestamp
  let now = Date.now()
  while(nextTs < now) {
    fireTimer(timersQueue.shift())
    if(timersQueue.length == 0) {
      timersLoopStarted = false
      return
    }
    nextTs = timersQueue[0].timestamp
  }
  let delay = nextTs - Date.now()
  if(delay > 1000) delay = 1000
  await maybeLoadMore()
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

async function maybeLoadMore() {
  if(!(lastLoadTime - Date.now() < loadMoreAfter)) return
  let loadTime = Date.now() + queueDuration
  let nextTimersCursor = await evs.db.run(
      r.table('timers')
          .between(lastLoadTime, loadTime, { index: 'timestamp' })
          .orderBy({ index: 'timestamp' })
  )
  let timers = await nextTimersCursor.toArray()
  lastLoadTime = loadTime
  for(let timer of timers) {
    insertTimer(timer)
  }
}

async function checkIfThereIsMore() {
  if(timersLoopStarted) return // loop started
  console.log("CHECK IF THERE IS MORE?")
  let loadTime = Date.now() + queueDuration
  let nextTimersCursor = await evs.db.run(
      r.table('timers')
          .between(r.minval, loadTime, { index: 'timestamp' })
          .orderBy({ index: 'timestamp' })
  )
  let timers = await nextTimersCursor.toArray()
  lastLoadTime = loadTime
  appendTimers(timers)
  if(!timersLoopStarted) startTimersLoop()
}


async function startTimers() {
  console.error("START TIMERS")
  await evs.db.run(r.tableCreate('timers')).catch(err => {})
  await evs.db.run(r.table('timers').indexCreate('timestamp')).catch(err => {})

  console.error("TIMERS?")

  let loadTime = Date.now() + queueDuration
  let nextTimersCursor = await evs.db.run(
      r.table('timers')
          .between(r.minval, loadTime, { index: 'timestamp' })
          .orderBy({ index: 'timestamp' })
  )
  console.error("TIMERS?")
  let timers = await nextTimersCursor.toArray()
  console.error("NEXT TIMERS", timers)
  lastLoadTime = loadTime
  appendTimers(timers)
  if(!timersLoopStarted) startTimersLoop()
}

function runTimerAction(timer) {
  console.error("RUN ACTION", timer)
  if(timer.command) return evs.call(timer.service, timer.command, { ...(timer.origin || {}), through: "timer" })

  if(timer.trigger) {
    if(timer.service) return evs.triggerService(timer.service, timer.trigger, "timer")
      else return evs.trigger(timer.trigger, "timer")
  }
  return new Promise((resolve, reject) => resolve(true))
}

function insertTimer(timer) {
  timersById.set(timer.id, timer)
  for(let i = 0; i < timersQueue; i++) {
    if(timer.timestamp < timersQueue[i].timestamp) {
      timersQueue.splice(i, 0, timer)
      return;
    }
  }
  timersQueue.push(timer)
  if(!timersLoopStarted) {
    startTimersLoop()
  } else if(timer.timestamp - Date.now() < 1000) {
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

evs.registerTriggers({

  createTimer( data, emit) {
    let timer = data.timer
    let timestamp = timer.timestamp
    let loops = timer.loops || 0
    let timerId = timer.id || uuid.v4()
    let maxRetries = timer.maxRetries || 0
    let retryDelay = timer.retryDelay || 5 * 1000
    let interval = timer.interval || 0
    if(loops > 0 && interval ==0) throw evs.error("impossibleTimer")
    const props = {
      ...timer, timestamp, loops, interval, timerId, maxRetries, retryDelay, retries: 0
    }
    emit([{
      type: "timerCreated",
      timer: timerId,
      data: props
    }])
    if(timestamp < Date.now() + queueDuration) {
      insertTimer({ ...props , id: timerId })
    }
    return timerId
  },

  cancelTimer({ timerId }, emit) {
    return evs.db.run(r.table("timers").get(timerId)).then(
      timerRow => {
        if(!timerRow) throw evs.error("notFound")
        emit([{
          type: "timerCanceled", timer: timerId
        }])
        removeTimer(timerId)
        return true
      }
    )
  }

})

evs.registerCommands({

  create( data , emit) {
    let timer = data.timer
    let timestamp = timer.timestamp
    let loops = timer.loops || 0
    let timerId = timer.id || uuid.v4()
    let maxRetries = timer.maxRetries || 0
    let retryDelay = timer.retryDelay || 5 * 1000
    let interval = timer.interval || 0
    if(loops > 0 && interval == 0) throw evs.error("impossibleTimer")
    const props = {
      ...timer, timestamp, loops, interval, timerId, maxRetries, retryDelay, retries: 0
    }
    emit([{
      type: "timerCreated",
      timer: timerId,
      data: props
    }])
    if(timestamp < Date.now() + queueDuration) {
      insertTimer({ ...props , id: timerId })
    }
    return timerId
  },

  cancel({ timerId }, emit) {
    return evs.db.run(r.table("timers").get(timerId)).then(
      timerRow => {
        if(!timerRow) throw evs.error("notFound")
        emit([{
          type: "timerCanceled", timer: timerId
        }])
        removeTimer(timerId)
        return true
      }
    )
  }

})

evs.registerEventListeners({
  queuedBy: 'timer',

  timerCreated({ timer, data, origin }) {
    return evs.db.run(
      r.table("timers").insert({
        id: timer,
        ...data,
        origin: { ...origin, ...( data.origin || {} ) }
      })
    )
  },
  timerCanceled({ timer }) {
    return evs.db.run(
        r.table("timers").get(timer).delete()
    )
  },
  timerFinished({ timer }) {
    return evs.db.run(
        r.table("timers").get(timer).delete()
    )
  },
  timerFired({ timer, timestamp }) {
    return evs.db.run(
      r.table("timers").get(timer).update(
        row => ({ loops: row("loops").sub(1), timestamp })
      )
    )
  },
  timerFailed({ timer, timestamp }) {
    return evs.db.run(
      r.table("timers").get(timer).update(
        row => ({ retries: row("retries").add(1), timestamp })
      )
    )
  }

})

require("../config/metricsWriter.js")('timer', () => ({

}))

process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
})

evs.start()
