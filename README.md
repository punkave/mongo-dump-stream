mongodump-stream
================

<a href="http://apostrophenow.org/"><img src="https://raw.githubusercontent.com/punkave/mongo-dump-stream/master/logos/logo-box-madefor.png" align="right" /></a>

## What's it do?

Let's pipe an entire MongoDB database through the shell:

```
npm install -g mongo-dump-stream

mongo-dump-stream mongodb://host:port/db1 |
mongo-load-stream mongodb://host:port/db2
```

Not quite useful yet? Look what happens when we pipe it over ssh:

```
mongo-dump-stream mongodb://host:port/db1 |
ssh user@server mongo-load-stream mongodb://host:port/db2
```

Bam! We just sent our database over a securely encrypted connection without creating [huge temporary folders](http://docs.mongodb.org/manual/reference/program/mongodump/).

Still not cool? Let's compress the data in transit to speed the transfer:

```
mongo-dump-stream mongodb://host:port/db1 | gzip -c |
ssh user@server "gunzip -c | mongo-load-stream mongodb://host:port/db2"
```

## I still don't get it...

This module lets you:

1. Dump an entire MongoDB database to standard output, or any other stream, without creating enormous temporary files.

2. Load that stream into another database.

3. Do these things in your own node app, or at the command line.

## Command line tools

Two handy command line utilities are provided: `mongo-dump-stream`, which takes a MongoDB URL and dumps that database to standard output, and `mongo-load-stream`, which takes a MongoDB URL and loads standard input into that database.

**mongo-load-stream will erase the current contents of your database.** It completely replaces it with the contents of the database arriving at standard input.

## API

Let's dump the database attached to an existing database connection opened with the [mongodb native](http://npmjs.org/packages/mongodb) module and send it to standard output:

```javascript
var mds = require('mongo-dump-stream');

// Use existing connection made with the mongodb module
return mds.dump(db, function(err) {
  if (!err) { // Everything  was sent }
});

// OR, use a URI
return mds.dump('mongodb://localhost:27017/mydatabase', function(err) {
  if (!err) { // Everything  was sent }
});

```

Now let's dump that database to a file instead (note the extra argument to `mds.dump`):

```javascript
var fs = require('fs');
var mds = require('mongo-dump-stream');
var out = fs.createWriteStream('/path/to/file.db');
return mds.dump(db, out, function(err) {
  if (!err) { // Everything was sent }
});
```

Now let's receive it from standard input:

```javascript
var mds = require('mongo-dump-stream');

// use existing database connection
return mds.load(db, function(err) {
  if (!err) { // Everything arrived }
});

// Use a URI
return mds.load('mongodb://localhost:27017/my-other-database', function(err) {
  if (!err) { // Everything arrived }
});
```

We can also read from a file:

```javascript
var mds = require('mongo-dump-stream');
var fin = fs.createReadStream('/path/to/file.db');

return mds.load(db, fin, function(err) {
  if (!err) { // Everything arrived }
});
```

`mongo-dump-stream` restores indexes as well as data.

## Alternatives

The standard `mongodump` and `mongorestore` utilities are obviously proven choices. They are also faster, because they are written in native code. However, they can't write an entire database to a single file, so they can't be piped. Also, they don't accept a `mongodb://` URI.

## Format notes

`mongo-dump-stream` writes a stream of BSON (not JSON) objects, for full compatibility with mongodb.

## About P'unk Avenue and Apostrophe

`mongo-dump-stream` was created at [P'unk Avenue](http://punkave.com) for use in many projects built with Apostrophe, an open-source content management system built on node.js. If you like `mongo-dump-stream` you should definitely [check out apostrophenow.org](http://apostrophenow.org).

## Support

Feel free to open issues on [github](http://github.com/punkave/mongo-dump-stream).

<a href="http://punkave.com/"><img src="https://raw.githubusercontent.com/punkave/mongo-dump-stream/master/logos/logo-box-builtby.png" /></a>

## Changelog

### CHANGES IN 0.1.1

Corrected examples and added more. No code changes.

### CHANGES IN 0.1.0

Initial release (with shiny unit tests, of course).

## LICENSE

Copyright (c) 2015 P'unk Avenue LLC

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
