const test = require('blue-tape')
const r = require('rethinkdb')
const testUtils = require('rethink-event-sourcing/tape-test-utils.js')
const crypto = require('crypto')
const { exec } = require('child_process');

test('Timer service restarts', t => {
  t.plan(6)
  let conn

  testUtils.connectToDatabase(t, r, (connection) => conn = connection)

  let userId = crypto.randomBytes(24).toString('hex')

  let timerId

  t.test('create empty timer', t => {
    t.plan(2)

    testUtils.runCommand(t, r, 'timer', {
      type: 'create',
      timer: {
        timestamp: Date.now() + 1*10*1000,
        service: null,
        command: null
      }
    }, (cId) => {
    }).then(
        result => timerId = result
    )

    t.test('check if timer exists', t => {
      t.plan(1)
      setTimeout(() => {
        r.table('timers').get(timerId).run(conn).then(
            timerRow => {
              if (timerRow) t.pass('timer exists')
              else t.fail('timer not found')
            }
        ).catch(t.fail)
      }, 300)
    })
  })

  t.test('restart timer service', t => {
    t.plan(1)
    exec('pm2 restart timer-service', (err, stdout, stderr) => {
      if(err) return t.fail(stderr)
      t.pass(stdout)
    })
  })

  t.test('check if timer exists', t => {
    t.plan(1)
    setTimeout(() => {
      r.table('timers').get(timerId).run(conn).then(
          timerRow => {
            if (timerRow) t.pass('timer exists')
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
    }, 1*10*1000 + 500)
  })

  t.test('close connection', t => {
    conn.close(() => {
      t.pass('closed')
      t.end()
    })
  })
})