/*


db = DataBlobs(db, metadb, function (stream, cb) {
  if(stream.meta) {
    blah blah blah

    instr
      .pipe(convert())
      .pipe(out())
      .on('error', cb)
      .on('close', cb)
  }
})
//will save the file in content store,
//remember the timestamp of the upload
//if this object has not already been added,
//will call the processing builder
//and when that write stream closes
//save that that file has been imported.
xlsxStream.pipe(db.createDataBlobStream(metadata))
*/
//and so each object is handled idempotently.
//AND you can rebuild the ENTIRE database from the saved files.

//AH... so that means that the done items should be stored in a
//append-only file. so that you can delete the main database and
//start over...

//or should the blobdb just be another db?
//you can always pass in json-logdb! or a sublevel if you don't
//care about deleteability.

var pl = require('pull-level')
var pull = require('pull-stream')
var peek = require('level-peek')
var timestamp = require('monotonic-timestamp')
var through = require('through')
var pushable = require('pull-pushable')
var cat = require('pull-cat')

module.exports = function (db, metadb, cas, handler) {

  if(!handler)
    cas = metadb, handler = cas, metadb = null
  if(!metadb)
    metadb = db.sublevel('data-blobs', {encoding: 'json'})

  function processData (meta, cb) {
    db.emit('processing', meta)
    var stream = cas.getStream(meta.hash)
    function done (err) {
      if(_cb) _cb(err)
      if(cb)   cb(err)
    }
    var _cb = meta._cb
    delete meta._cb
    stream.meta = meta

    handler.call(db, stream, function (err) {
      if(err) done(err)
      else    metadb.put('done-' + meta.timestamp, meta, done)
    })
  }

  // a trivially simple work queue
  // initialize is processed first.
  // (we scan the database and check if
  // there is nothing missing from last run)
  // then initialize ends, and we read from the work queue.
  // this just reads forever with no end,
  // it it just processes incoming files one at a time.
  // this gaurantees that input files are always processed in the same order.

  var initialize = pull.defer()
  var workQueue = pushable()

  // it might be good to know the current state of the queeu?

  pull(
    cat([initialize, workQueue]),
    pull.asyncMap(processData),
    pull.drain()
  )


  db.createDataBlobStream = function (meta) {
    var stream = through(null, null, {autoDestroy: false})
    function done(err) {
      if(err) stream.emit('error', err)
      else    stream.emit('close')
    }
    stream.pipe(cas.addStream())
      .on('close', function () {
        meta = meta || {}
        meta.timestamp = timestamp()
        meta.hash = this.hash
        metadb.get('hash-' + meta.hash, function (err, _meta) {
          if(_meta) return done()

          metadb.batch([
            { key: 'hash-'+meta.hash, value: meta, type: 'put', encoding: 'json' },
            { key: 'ts-'+meta.timestamp, value: meta, type: 'put', encoding: 'json' },
          ], function (err) {
            if(err) return cb ? cb(err) : db.emit('error', err)
            meta._cb = done
            workQueue.push(meta)
          })
        })
      })
    return stream
  }
  var rebuild = false

  //look in the database, and reprocess anything that did not complete last time.
  //this is done serially, so that datablobs are imported in the same order.
  peek.last(metadb, {start: 'done-!', end: 'done-~'}, function (err, latest) {
    if(!latest) return initialize.resolve(pull.values([]))//we are done already!
    var start = 'ts-' + latest.timestamp
    initialize.resolve(pull(
      pl.read({start: start, end: 'ts-~', keys: false}),
      pull.filter(function (meta) {
        return meta.key > meta.timestamp
      })
    ))
  })

  db.reprocess = function (cb) {
    pull(
      //stream ts section of database... so it's processed in order.
      pl.read(metadb, {start: 'ts-!', end: 'ts-~', keys: false}),
      //write one at a time, _cb is triggered when that meta is written.
      pull.asyncMap(function (meta, cb) {
        meta._cb = cb
        workQueue.push(meta)
      }),
      //once ALL the things have been written, cb.
      pull.drain(null, cb)
    )
  }

  return db
}

