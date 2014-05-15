# level-datablobs

loads blobs of data into leveldb reliably.

A `datablob` is any file containing that want to turn into many pieces of data,
for example a spreadsheet that becomes many rows.

`level-datablobs` provides a streaming api, and tracks which blobs have been processed.
Each blob is processed in order, and can be immutably reprocessed.

## Exampls

``` js

var db = DataBlobs(
  // a database to store the data.
  level(path.join(dirname, 'db'), {encoding: 'json'}),

  // a database to store metadata about what has been processed.
  level(path.join(dirname, 'meta'), {encoding: 'json'}),

  // a content-addressable-store to store the raw datablobs
  cas(path.join(dirname, 'blobs')),

  //a function that puts a datablob into the database.
  function (stream, cb) {
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
      //see levelup issue https://github.com/rvagg/node-levelup/issues/247
      .pipe(toStream(pl.write(db)).on('close', cb))
  }
)


```


## License

MIT
