const test = require('blue-tape')
const r = require('rethinkdb')
const testUtils = require('rethink-event-sourcing/tape-test-utils.js')
const crypto = require('crypto')

test('Timer service', t => {

  t.plan(6)

  let conn

  testUtils.connectToDatabase(t, r, (connection) => conn = connection)

  let sessionId = crypto.randomBytes(24).toString('hex')

  t.test('create empty timer', t => {
    t.plan(3)

    let timerId

    testUtils.runCommand(t, r, 'timer', {
      type: 'create',
      timer: {
        timestamp: Date.now() + 1000,
        service: null,
        command: null
      }
    }, (cId) => { }).then(
      result => timerId = result
    )

    t.test('check if timer exists', t=> {
      t.plan(1)
      setTimeout(()=>{
        r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if(timerRow) t.pass('timer exists')
            else t.fail('timer not found')
          }
        ).catch(t.fail)
      }, 300)
    })

    t.test('check if timer removed after timestamp', t=> {
      t.plan(1)
      setTimeout(()=>{
        r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if(!timerRow) t.pass('timer removed')
            else t.fail('timer still exits')
          }
        ).catch(t.fail)
      }, 1300)
    })

  })

  let timerId

  t.test('create timer that will create session', t => {
    t.plan(4)

    testUtils.runCommand(t, r, 'timer', {
      type: 'create',
      timer: {
        timestamp: Date.now() + 1000,
        service: "session",
        command: {
          type: 'createSessionIfNotExists',
          session: sessionId
        }
      }
    }, (cId) => { }).then(
      result => timerId = result
    )

    t.test('check if timer exists', t=> {
      t.plan(1)
      setTimeout(()=>{
        r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if(timerRow) t.pass('timer exists')
            else t.fail('timer not found')
          }
        ).catch(t.fail)
      }, 300)
    })

    t.test('check if session exists', t=> {
      t.plan(1)
      setTimeout(()=>{
        r.table('session').get(sessionId).run(conn).then(
          sessionRow => {
            if(sessionRow) t.pass('session exists')
            else t.fail('session not found')
          }
        ).catch(t.fail)
      }, 1500)
    })

    t.test('check if timer removed after timestamp', t=> {
      t.plan(1)
      setTimeout(()=>{
        r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if(!timerRow) t.pass('timer removed')
            else t.fail('timer still exits')
          }
        ).catch(t.fail)
      }, 500)
    })

  })

  t.test('create empty interval', t => {
    t.plan(3)

    testUtils.runCommand(t, r, 'timer', {
      type: 'create',
      timer: {
        timestamp: Date.now() + 1000,
        loops: 10,
        interval: 1000,
        service: null,
        command: null
      }
    }, (cId) => { }).then(
      result => timerId = result
    )

    t.test('check if timer exists', t=> {
      t.plan(2)
      setTimeout(()=>{
        r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if(timerRow) t.pass('timer exists')
            else t.fail('timer not found')
            t.equal(timerRow.loops, 10, "Loops as it started")
          }
        ).catch(t.fail)
      }, 300)
    })

    t.test('check if timer loops decrased after timestamp', t=> {
      t.plan(2)
      setTimeout(()=>{
        r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if(timerRow) t.pass('timer exists')
            else t.fail('timer not found')
            t.equal(timerRow.loops, 9, "Loops decrased")
          }
        ).catch(t.fail)
      }, 1200)
    })

  })

  t.test("remove interval", t=> {
    t.plan(2)

    testUtils.runCommand(t, r, 'timer', {
      type: 'cancel', timerId
    }, (cId) => { }).then(
      result => timerId = result
    )

    t.test('check if timer removed after cancellation', t=> {
      t.plan(1)
      setTimeout(()=>{
        r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if(!timerRow) t.pass('timer removed')
            else t.fail('timer still exits')
          }
        ).catch(t.fail)
      }, 500)
    })
  })


  t.test('close connection', t => {
    conn.close(() => {
      t.pass('closed')
      t.end()
    })
  })

})