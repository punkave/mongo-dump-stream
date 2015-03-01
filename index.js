/* jshint node:true */

var async = require('async');
var _ = require('lodash');
var bson = require('bson');
var BSON = bson.BSONPure.BSON;
var fs = require('fs');
var BSONStream = require('bson-stream');
var mongodb = require('mongodb');

module.exports = {
  // If you leave out "stream" it'll be stdout
  dump: function(dbOrUri, stream, callback) {
    if (arguments.length === 2) {
      callback = stream;
      stream = undefined;
    }
    if (!stream) {
      stream = process.stdout;
    }
    var db;
    var out = stream;
    write({
      type: 'mongo-dump-stream',
      version: '1'
    });
    return async.series({
      connect: function(callback) {
        if (typeof(dbOrUri) !== 'string') {
          // Already a mongodb connection
          db = dbOrUri;
          return setImmediate(callback);
        }
        return mongodb.MongoClient.connect(dbOrUri, function(err, _db) {
          if (err) {
            return callback(err);
          }
          db = _db;
          return callback(null);
        });
      },
      getCollections: function(callback) {
        return db.collections(function(err, _collections) {
          if (err) {
            return callback(err);
          }
          collections = _collections;
          return callback(null);
        });
      },
      dumpCollections: function(callback) {
        return async.eachSeries(collections, function(collection, callback) {
          if (collection.collectionName.match(/^system\./)) {
            return setImmediate(callback);
          }
          return async.series({
            getIndexes: function(callback) {
              return collection.indexInformation({ full: true }, function(err, info) {
                if (err) {
                  return callback(err);
                }
                write({
                  type: 'collection',
                  name: collection.collectionName,
                  indexes: info
                });
                return callback(null);
              });
            },
            getDocuments: function(callback) {
              var cursor = collection.find();
              iterate();
              function iterate() {
                return cursor.nextObject(function(err, item) {
                  if (err) {
                    return callback(err);
                  }
                  if (!item) {
                    write({
                      type: 'endCollection'
                    });
                    return callback(null);
                  }
                  write({
                    type: 'document',
                    document: item
                  });
                  iterate();
                });
              }
            },
          }, callback);
        }, callback);
      },
      endDatabase: function(callback) {
        write({
          type: 'endDatabase'
        });
        return setImmediate(callback);
      }
    }, function(err) {
      if (err) {
        return callback(err);
      }
      return callback(null);
    });
    function write(o) {
      out.write(BSON.serialize(o, false, true, false));
    }
  },
  // If you leave out "stream" it'll be stdin
  load: function(dbOrUri, stream, callback) {
    var db;
    if (arguments.length === 2) {
      callback = stream;
      stream = undefined;
    }
    if (!stream) {
      stream = process.stdin;
    }
    var bin = stream.pipe(new BSONStream());

    bin.on('error', function(err) {
      return callback(err);
    });

    var state = 'start';

    var collection;

    // State machine

    var processors = {
      start: function(item, callback) {
        if (typeof(item) !== 'object') {
          return callback(new Error('First BSON element is not an object'));
        }
        if (item.type !== 'mongo-dump-stream') {
          return callback(new Error('type property is not mongo-dump-stream'));
        }
        if (item.version !== '1') {
          return callback(new Error('Incoming mongo-dump-stream is of a newer version, bailing out'));
        }
        state = 'collection';
        return callback(null);
      },
      collection: function(item, callback) {
        if (item.type === 'endDatabase') {
          state = 'done';
          return callback(null);
        }
        if (item.type !== 'collection') {
          return callback(new Error('collection expected'));
        }
        var name = item.name;
        var indexes = item.indexes;
        async.series({
          create: function(callback) {
            return db.collection(name, function(err, _collection) {
              if (err) {
                return callback(err);
              }
              collection = _collection;
              return callback(null);
            });
          },
          indexes: function(callback) {
            return async.eachSeries(item.indexes, function(item, callback) {
              if (item.name === '_id_') {
                // automatic
                return setImmediate(callback);
              }
              return collection.ensureIndex(item.key, _.omit(item, 'ns', 'key'), callback);
            }, callback);
          }
        }, function(err) {
          if (err) {
            return callback(err);
          }
          state = 'documents';
          return callback(null);
        });
      },
      documents: function(item, callback) {
        if (item.type === 'endCollection') {
          state = 'collection';
          return setImmediate(callback);
        }
        if (item.type !== 'document') {
          return callback(new Error('Document expected'));
        }
        return collection.insert(item.document, callback);
      }
    };

    return async.series({
      connect: function(callback) {
        if (typeof(dbOrUri) !== 'string') {
          // Already a mongodb connection
          db = dbOrUri;
          return setImmediate(callback);
        }
        return mongodb.MongoClient.connect(dbOrUri, function(err, _db) {
          if (err) {
            return callback(err);
          }
          db = _db;
          return callback(null);
        });
      },
      dropCollections: function(callback) {
        return db.collections(function(err, collections) {
          if (err) {
            return callback(err);
          }
          return async.eachSeries(collections, function(item, callback) {
            if (item.collectionName.match(/^system\./)) {
              return setImmediate(callback);
            }
            return item.drop(callback);
          }, callback);
        });
      },
      loadCollections: function(callback) {
        var active = false;
        bin.on('readable', function() {
          if (active) {
            // This should never happen, but I've received
            // another readable event on a BSONStream when
            // the previous one hasn't yet been exhausted.
            // It would be Bad to start doubling up and
            // not handling the objects in series. Fortunately we
            // get another readable event when the buffer
            // is truly exhausted.
            return;
          }
          active = true;
          var item;
          // Keep calling bin.read() and invoking
          // processors, but make sure we do it
          // asynchronously. Otherwise this would
          // be a simple while loop.
          readNext();
          function readNext() {
            if (state === 'done') {
              return setImmediate(callback);
            }
            var item = bin.read();
            if (item === null) {
              active = false;
              return;
            }
            if (typeof(item) !== 'object') {
              return new Error('Object expected');
            }
            return processors[state](item, function(err) {
              if (err) {
                return callback(err);
              }
              setImmediate(readNext);
            });
          }
        });
      }
    }, callback);
  }
};

