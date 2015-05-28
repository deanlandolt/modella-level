var Secondary = require('level-secondary')
var through = require('through2')

var level = module.exports = function(db) {
  if (typeof db == 'undefined') {
    throw new Error('you must pass in a leveldb instance')
  }

  return function(Model) {
    var store = Model.store = db.sublevel(Model.modelName)

    //
    // add store implementations to host
    //
    if (!db.isClient) {
      store.methods.createLiveStream = { type: 'readable' }
    }

    //
    // add nodel method implementations
    //
    Model.save = level.save
    Model.update = level.update
    Model.remove = level.remove
    Model.find = level.find
    Model.findBy = level.findBy
    Model.removeBy = level.removeBy
    Model.tail = level.tail

    // TODO: remote client indexes
    var indexedAttrs = []

    Model.once('initialize', function () {
      for (var attr in Model.attrs) {
        if (Model.attrs[attr].index) {
          store[('by' + attr).toLowerCase()] = Secondary(store, attr)
        }
      }
    })

    return Model
  }
}

level.save = function (fn) {
  var self = this

  if (!this.model.primaryKey) {
    return fn(new Error('No primary key set on model'))
  }

  this.model.store.put(this.primary(), this.toJSON(), {valueEncoding: 'json'}, function(err) {
    if (err) {
      return fn(err)
    }

    fn(null, self.toJSON())
  })
}

level.update = level.save

level.remove = function (fn) {
  this.store.del(this.primary(), fn)
}

level.find = function (id, fn) {
  var self = this

  this.store.get(id, function(err, result) {
    if (err && err.notFound) {
      return fn(new Error('unable to find ' + self.modelName + ' with id: ' + id), false)
    }
    if (err) {
      return fn(err)
    }
    fn(null, new self(result))
  })
}

level.findBy = function (field, value, fn) {
  var self = this

  var getBy = ('by' + field).toLowerCase()

  if (typeof this.store[getBy] == 'undefined') {
    return fn(new Error('field does not exist'))
  }

  this.store[getBy].get(value, function(err, value2) {
    if (err) {
      return fn(err)
    }

    fn(err, new self(value2))
  })
}

level.removeBy = function (field, value, fn) {
  var self = this

  var getBy = ('by' + field).toLowerCase()

  if (typeof this.store[getBy] == 'undefined') {
    return fn(new Error('field does not exist'))
  }

  this.store[getBy].del(value, function(err, value2) {
    if (err) {
      return fn(err)
    }

    fn(err, null)
  })
}

level.tail = function (options) {
  //
  // create a transform to lift each result record into a model
  //
  var self = this
  var transform = through.obj(function (d, enc, cb) {
    try {
      d.value && (d.value = new self(d.value))
      cb(null, d)
    }
    catch (e) {
      cb(e)
    }
  })

  return this.store.createLiveStream(options).pipe(transform)
}
