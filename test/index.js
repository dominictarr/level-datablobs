var path    = require('path')
var osenv   = require('osenv')
var tape    = require('tape')
var level   = require('level')
var cas     = require('content-addressable-store')
var rimraf  = require('rimraf')
var mkdirp  = require('mkdirp')
var fs      = require('fs')
var through = require('through')
var csv     = require('csv-stream')
var toStream = require('pull-stream-to-stream')
var pl      = require('pull-level')

var DataBlobs = require('../')

function create (dirname, clean, handle) {
  if(!handle) handle = clean, clean = true

  if(clean) rimraf.sync(dirname)
  mkdirp.sync(dirname)
  var db = level(path.join(dirname, 'db'), {encoding: 'json'})
  var metadb = level(path.join(dirname, 'meta'), {encoding: 'json'})

  db.metadb = metadb
  return DataBlobs(
    db, metadb,
    cas(path.join(dirname, 'blobs')),
    handle
  )
}

function first (obj) {
  for(var k in obj)
    return obj[k]
}

function  processCsv (stream, cb) {
  var db = this
  stream
    .pipe(csv.createStream())
    .pipe(through(function (data) {
      var obj = {}
      for(var k in data) {
        var value = data[k].trim()
        obj[k.trim()] = isNaN(value) ? value : + value
      }
      this.queue({key: first(obj), value: obj, type: 'put'})
    }))
    //using a pull stream to write
    //because levelup's write stream emits close too early.
    .pipe(toStream(pl.write(db, cb)))
}

tape ('simple', function (t) {
  var db = create(path.join(osenv.tmpdir(), 'datablobs-simple'), processCsv)

  //now load a file into the database.

  fs.createReadStream(path.join(__dirname, 'fixtures', 'example1.csv'))
    .pipe(db.createDataBlobStream({type: 'example'}))
    .on('close', function () {
      //now the stream has been fully written!

      db.get('0', function (err, value) {
        if(err) throw err
        console.error(value)
        t.equal(value.foo, 99)
        t.end()
      })

    })
})

tape ('idempotent', function (t) {
  var db = create(path.join(osenv.tmpdir(), 'datablobs-idempotent'), processCsv)

  //now load a file into the database.

  fs.createReadStream(path.join(__dirname, 'fixtures', 'example1.csv'))
    .pipe(db.createDataBlobStream({type: 'example'}))
    .on('close', function () {
      //now the stream has been fully written!

      function shouldNotHappen (data) {
        t.notOk(data)
        t.ok(false)
      }

      db.on('put', shouldNotHappen)
      db.on('batch', shouldNotHappen)
      db.on('del', shouldNotHappen)

      fs.createReadStream(path.join(__dirname, 'fixtures', 'example1.csv'))
        .pipe(db.createDataBlobStream({type: 'example'}))
        .on('close', function () {

          db.get('0', function (err, value) {
            t.equal(value.foo, 99)
            t.end()
          })
        })
    })
})

tape('reprocess', function (t) {

  var db = create(path.join(osenv.tmpdir(), 'datablobs-reprocess'), processCsv)

  //now load a file into the database.
  var seen = []
  db.on('processing', function (meta) {
    seen.push(meta)
  })
  fs.createReadStream(path.join(__dirname, 'fixtures', 'example1.csv'))
    .pipe(db.createDataBlobStream({type: 'example'}))
    .on('close', function () {
      db.reprocess(function (err) {
        t.equal(seen.length, 2)
        t.end()
      })
    })
})

tape('recover', function (t) {
  var db = create(path.join(osenv.tmpdir(), 'datablobs-recover'), function (stream) {
    stream.resume()
    db.close(function () {
      db.metadb.close(function () {
        console.log('close')
        //opening a new version of this database should start processing stuff that wasn't finished.
        var db = create(path.join(osenv.tmpdir(), 'datablobs-recover'), false, processCsv)
        db.on('processing', function (meta) {
          console.log('meta', meta)
          t.ok(meta)
          t.end()
        })
      })
    })
  })

  fs.createReadStream(path.join(__dirname, 'fixtures', 'example1.csv'))
    .pipe(db.createDataBlobStream({type: 'example'}))
})
