
/*
Redis Extended Simple Message Queue

The MIT License (MIT)
Copyright © 2016 Paulo Ferreira <pf at sourcenotes.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in the
Software without restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
var EventEmitter, FlakeID, Message, Queue, System, _, _DEFAULTS, _VALIDATORS, genID, intFormat, nullOnEmpty, redis, validateMID, validateQNAME,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

redis = require("redis");

FlakeID = require("flake-idgen");

intFormat = require('biguint-format');

_ = require("lodash");

EventEmitter = require("events").EventEmitter;

_VALIDATORS = {
  id: /^([A-Z0-9:]){16}$/,
  qname: /^([a-zA-Z0-9_-]){1,160}$/
};

_DEFAULTS = {
  redis: {
    host: "127.0.0.1",
    port: 6379,
    options: {
      password: null,
      db: 0
    }
  },
  queue: {
    htimeout: 30,
    etimeout: -1,
    plimit: -1
  }
};

nullOnEmpty = function(string) {
  if (_.isString(string)) {
    string = string.trim();
    if (string.length) {
      return string;
    }
  }
  return null;
};

genID = function() {
  var newID;
  newID = new FlakeID;
  return intFormat(newID.next(), 'hex').toUpperCase();
};

validateQNAME = function(name) {
  name = nullOnEmpty(name);
  if (name == null) {
    throw new Error("MISSING Queue Name [name]");
  } else if (!_VALIDATORS.qname.test(name)) {
    throw new Error("INVALID Queue Name [name]");
  }
  return name;
};

validateMID = function(id) {
  id = nullOnEmpty(id);
  if (id == null) {
    throw new Error("MISSING Message ID [id]");
  } else if (!_VALIDATORS.id.test(id)) {
    throw new Error("INVALID Message ID [id]");
  }
  return id;
};

Message = (function(superClass) {
  extend(Message, superClass);

  Message._redisProps = ["queue", "message", "hidden", "pcount", "htimeout", "etimeout", "plimit", "created", "modified"];

  Message.__load = function(system, id, cb) {
    var key;
    key = system._systemKey(["messages", id]);
    return system._redis.hmget(key, Message._redisProps, function(error, resp) {
      var m;
      if (error != null) {
        return cb(error);
      }
      if (resp.length) {
        m = Message.getInstance(system);
        m._id = id;
        m._props["queue"] = resp[0];
        m._props["message"] = resp[1];
        m._props["hidden"] = !!resp[2];
        m._props["pcount"] = resp[3] != null ? parseInt(resp[3], 10) : null;
        m._props["htimeout"] = resp[4] != null ? parseInt(resp[4], 10) : null;
        m._props["etimeout"] = resp[5] != null ? parseInt(resp[5], 10) : null;
        m._props["plimit"] = resp[6] != null ? parseInt(resp[6], 10) : null;
        m._props["created"] = parseInt(resp[7], 10);
        m._props["modified"] = parseInt(resp[8], 10);
        return cb(null, m);
      }
      return cb(null, null);
    });
  };

  Message.__create = function(system, queue, message, props, cb) {
    if (props == null) {
      props = {};
    }
    redis = system._redis;
    return redis.time(function(err, rtime) {
      var commands, created, hmset, id;
      if (typeof error !== "undefined" && error !== null) {
        return cb(error, null);
      }
      id = genID();
      hmset = ["HMSET", system._systemKey(["messages", id])];
      props = {
        hidden: false
      };
      hmset.push("hidden", false);
      _.forOwn(props, function(value, key) {
        if (_.indexOf(Message._redisProps, key) >= 0) {
          return hmset.push(key, props[key] = value);
        }
      });
      created = parseInt(rtime[0], 10) * 1000000 + parseInt(rtime[1], 10);
      hmset.push("queue", props["queue"] = queue);
      hmset.push("message", props["message"] = message);
      hmset.push("created", props["created"] = created);
      hmset.push("modified", props["modified"] = created);
      commands = [hmset, ["ZADD", system._systemKey(["queue", queue, "M"]), created, id], ["HINCRBY", system._systemKey(["queue", queue, "P"]), "received", 1]];
      return redis.multi(commands).exec(function(error, rcmds) {
        var m;
        if (error != null) {
          return cb(error, null);
        }
        m = Message.getInstance(system);
        m._id = id;
        m._props = props;
        return cb(null, m);
      });
    });
  };

  Message.__delete = function(system, queue, id, cb) {
    var commands;
    redis = system._redis;
    commands = [["ZREM", system._systemKey(["queue", queue, "M"]), id], ["ZREM", system._systemKey(["queue", queue, "H"]), id], ["DEL", system._systemKey(["messages", id])]];
    return redis.multi(commands).exec(function(error, rcmds) {
      if (error != null) {
        return cb(error);
      }
      return cb(null, id);
    });
  };

  Message.__findInAll = function(system, id, active, cb) {
    return Message.__load(system, id, function(error, m) {
      if (error != null) {
        return cb(error);
      }
      if ((m != null) && (!active || !m._props.hidden)) {
        return cb(null, msg);
      }
      return cb(null, null);
    });
  };

  Message.__updateProperty = function(system, id, pname, pvalue, cb) {
    var key;
    key = system._systemKey(["messages", id]);
    return system._redis.hmset(key, pname, pvalue, function(error, resp) {
      if (error != null) {
        cb(error);
      }
      return cb(null, true);
    });
  };

  Message.prototype._id = null;

  Message.prototype._props = {};

  Message.getInstance = function(system) {
    return new Message(system);
  };

  function Message(system1) {
    this.system = system1;
  }

  Message.prototype.id = function() {
    return this._id;
  };

  Message.prototype.message = function() {
    return this._props.message;
  };

  Message.prototype.queue = function() {
    return this._props.queue;
  };

  Message.prototype.update = function(message, cb) {
    message = nullOnEmpty(message);
    if (message == null) {
      throw new Error("Parameter [message] is NOT a String or is an Empty String");
    }
    cb = _.isFunction(cb) ? cb : null;
    Message.__updateProperty(this.system, this._id, "message", message, (function(_this) {
      return function(error, ok) {
        var old;
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error, _this) : void 0;
        }
        if (ok) {
          old = _this._props["message"];
          _this._props["message"] = message;
          _this.emit("updated", _this, "message", old);
          return typeof cb === "function" ? cb(error, _this, "message", old) : void 0;
        } else {
          error = new Error("Failed to Update [message] in Message [" + _this._id + "]");
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error, _this) : void 0;
        }
      };
    })(this));
    return this;
  };

  Message.prototype.updateJSON = function(json, cb) {
    if (_.isPlainObject(json)) {
      return this.update(JSON.stringify(json), cb);
    }
    throw new Error("Parameter [json] is Missing or is not an Object");
  };

  Message.prototype["delete"] = function(cb) {
    Message.__delete(this.system, this._props.queue, this._id, (function(_this) {
      return function(error, id) {
        if (error != null) {
          _this.emit("error", error);
          if (typeof cb === "function") {
            cb(error);
          }
        }
        _this.emit("deleted", _this);
        return typeof cb === "function" ? cb(null, _this) : void 0;
      };
    })(this));
    return this;
  };

  Message.prototype.move = function(toqueue, cb) {
    toqueue = validateQNAME(toqueue);
    if (this._props.queue !== toqueue) {
      Queue.__exists(this.system, toqueue, (function(_this) {
        return function(error, exists) {
          if (error != null) {
            _this.emit("error", error);
            if (typeof cb === "function") {
              cb(error);
            }
          }
          if (!exists) {
            error = new Error("Destination queue [" + toqueue + "] does not exist");
            _this.emit("error", error);
            if (typeof cb === "function") {
              cb(error);
            }
          }
          return Queue.__removeMessage(_this.system, _this._props.queue, _this._id, function(error, id) {
            if (error != null) {
              _this.emit("error", error);
              if (typeof cb === "function") {
                cb(error);
              }
            }
            return Queue.__addMessage(_this.system, toqueue, _this._id, function(error, id) {
              if (error != null) {
                _this.emit("error", error);
                if (typeof cb === "function") {
                  cb(error);
                }
              }
              _this.emit("moved", _this);
              return typeof cb === "function" ? cb(null, _this) : void 0;
            });
          });
        };
      })(this));
    } else {
      this.emit("moved", this);
      if (typeof cb === "function") {
        cb(null, this);
      }
    }
    return this;
  };

  Message.prototype.hide = function(toqueue, cb) {
    var timeout;
    timeout = this._props.htimeout != null ? this._props.htimeout : _DEFAULTS.queue.htimeout;
    Queue.__hideMessage(this.system, this._props.queue, this._id, timeout, (function(_this) {
      return function(error, hidden) {
        if (error != null) {
          _this.emit("error", error);
          if (typeof cb === "function") {
            cb(error);
          }
        }
        _this.emit("hidden", _this);
        return typeof cb === "function" ? cb(null, _this) : void 0;
      };
    })(this));
    return this;
  };

  Message.prototype.refresh = function(cb) {
    var key;
    key = this.system._systemKey(["messages", this._id]);
    this.system._redis.hmget(key, Message._redisProps, (function(_this) {
      return function(error, resp) {
        if (error != null) {
          _this.emit("error", error);
          if (typeof cb === "function") {
            cb(error);
          }
        }
        if (resp.length) {
          _this._props["message"] = resp[1];
          _this._props["hidden"] = !!resp[2];
          _this._props["pcount"] = resp[3] != null ? parseInt(resp[3], 10) : null;
          _this._props["htimeout"] = resp[4] != null ? parseInt(resp[4], 10) : null;
          _this._props["etimeout"] = resp[5] != null ? parseInt(resp[5], 10) : null;
          _this._props["plimit"] = resp[6] != null ? parseInt(resp[6], 10) : null;
          _this._props["created"] = parseInt(resp[7], 10);
          _this._props["modified"] = parseInt(resp[8], 10);
          return cb(null, _this);
        }
        error = new Error("Invalid Message [" + _this._id + "]");
        _this.emit("error", error);
        return typeof cb === "function" ? cb(error) : void 0;
      };
    })(this));
    return this;
  };

  return Message;

})(EventEmitter);

Queue = (function(superClass) {
  extend(Queue, superClass);

  Queue._redisProps = ["htimeout", "etimeout", "plimit", "created", "modified", "received", "sent"];

  Queue.__list = function(system, cb) {
    return system._redis.smembers(system._systemKey("queues"), function(error, resp) {
      if (error != null) {
        return cb(error);
      }
      return cb(null, resp);
    });
  };

  Queue.__loadOrCreate = function(system, name, options, cb) {
    return Queue.__exists(system, name, function(error, exists) {
      if (error != null) {
        return cb(error, null);
      }
      if (exists) {
        return Queue.__load(system, name, cb);
      } else {
        return Queue.__create(system, name, options, cb);
      }
    });
  };

  Queue.__exists = function(system, name, cb) {
    var key;
    key = system._systemKey(["queue", name, "P"]);
    system._redis.exists(key, function(err, resp) {
      var exists;
      if (err != null) {
        return cb(err, null);
      }
      exists = !!resp;
      return cb(null, exists);
    });
    return this;
  };

  Queue.__load = function(system, name, cb) {
    var key;
    key = system._systemKey(["queue", name, "P"]);
    return system._redis.hmget(key, Queue._redisProps, function(error, resp) {
      var q;
      if (error != null) {
        return cb(error);
      }
      if (resp.length) {
        q = Queue.getInstance(system, name);
        q._htimeout = parseInt(resp[0], 10);
        q._etimeout = parseInt(resp[1], 10) || 0;
        q._plimit = parseInt(resp[2], 10) || 0;
        q._created = parseInt(resp[3], 10);
        q._modified = parseInt(resp[4], 10);
        q._received = parseInt(resp[5], 10);
        q._sent = parseInt(resp[6], 10);
        return cb(null, q);
      } else {
        error = new Error("Queue [" + name + "] does not exist");
        return cb(error);
      }
    });
  };

  Queue.__create = function(system, name, options, cb) {
    return system._redis.time(function(err, rtime) {
      var commands, hmset;
      if (err != null) {
        return cb(err, null);
      }
      hmset = ["HMSET", system._systemKey(["queue", name, "P"]), "htimeout", options.htimeout, "etimeout", options.etimeout, "plimit", options.plimit, "created", rtime[0], "modified", rtime[0], "received", 0, "sent", 0];
      commands = [hmset, ["SADD", system._systemKey("queues"), name]];
      return system._redis.multi(commands).exec(function(err, rset) {
        var q;
        if (err != null) {
          return cb(err);
        }
        q = Queue.getInstance(system, name);
        q._htimeout = options.htimeout;
        q._etimeout = options.etimeout;
        q._plimit = options.plimit;
        q._created = rtime[0];
        q._modified = rtime[0];
        q._received = 0;
        q._sent = 0;
        return cb(null, q);
      });
    });
  };

  Queue.__addMessage = function(system, queue, id, cb) {
    redis = system._redis;
    return redis.time(function(error, rtime) {
      var commands, hmset, modified;
      if (error != null) {
        return cb(error);
      }
      modified = parseInt(rtime[0], 10) * 1000000 + parseInt(rtime[1], 10);
      hmset = ["HMSET", system._systemKey(["messages", id]), "queue", queue, "hidden", 0, "modified", modified];
      commands = [hmset, ["ZADD", system._systemKey(["queue", queue, "M"]), modified, id], ["HINCRBY", _this.system._systemKey(["queue", queue, "P"]), "received", 1]];
      return system.server.multi(commands).exec(function(err, rcmds) {
        if (error != null) {
          return cb(error);
        }
        return typeof cb === "function" ? cb(null, id) : void 0;
      });
    });
  };

  Queue.__removeMessage = function(system, queue, id, cb) {
    redis = system._redis;
    return redis.time(function(error, rtime) {
      var commands, hmset, modified;
      if (error != null) {
        return cb(error);
      }
      modified = parseInt(rtime[0], 10) * 1000000 + parseInt(rtime[1], 10);
      hmset = ["HMSET", system._systemKey(["messages", id]), "queue", null, "modified", modified];
      commands = [hmset, ["ZREM", system._systemKey(["queue", queue, "M"]), id], ["ZREM", system._systemKey(["queue", queue, "H"]), id]];
      return system.server.multi(commands).exec(function(err, rcmds) {
        if (error != null) {
          return cb(error);
        }
        return typeof cb === "function" ? cb(null, id) : void 0;
      });
    });
  };

  Queue.__hideMessage = function(system, queue, id, timeout, cb) {
    redis = system._redis;
    return redis.time(function(error, rtime) {
      var commands;
      if (error != null) {
        return cb(error);
      }
      timeout = parseInt(rtime[0] + timeout, 10) * 1000000 + parseInt(rtime[1], 10);
      commands = [["ZREM", system._systemKey(["queue", queue, "M"]), id], ["ZADD", system._systemKey(["queue", queue, "H"]), timeout, id], ["HSET", system._systemKey(["messages", id]), "hidden", 1]];
      return redis.multi(commands).exec(function(error, rcmds) {
        if (error != null) {
          return cb(error);
        }
        return cb(null, true);
      });
    });
  };

  Queue.__findMessage = function(system, queue, id, active, cb) {
    return Message.__load(system, id, function(error, m) {
      if (error != null) {
        return cb(error);
      }
      if ((m != null) && (m._queue === queue)) {
        return cb(null, msg);
      }
      return cb(null, null);
    });
  };

  Queue.__pending = function(system, queue, cb) {
    redis = system._redis;
    return redis.time(function(error, rtime) {
      var key, time;
      if (error != null) {
        return cb(error);
      }
      time = parseInt(rtime[0], 10) * 1000000 + parseInt(rtime[1], 10);
      key = system._systemKey(["queue", queue, "M"]);
      return redis.zrangebyscore(key, "-inf", time, function(error, messages) {
        if (error != null) {
          return cb(error);
        }
        return cb(null, messages);
      });
    });
  };

  Queue.getInstance = function(system, name) {
    return new Queue(system, name);
  };

  function Queue(system1, name1) {
    this.system = system1;
    this.name = name1;
  }

  Queue.prototype.refresh = function(cb) {
    return this._load(cb);
  };

  Queue.prototype.post = function(message, cb) {
    cb = _.isFunction(cb) ? cb : null;
    this._post(message, (function(_this) {
      return function(error, m) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        _this.emit("message-new", m);
        return typeof cb === "function" ? cb(null, m) : void 0;
      };
    })(this));
    return this;
  };

  Queue.prototype.postJSON = function(json, cb) {
    if (_.isPlainObject(json)) {
      return this.post(JSON.stringify(json), cb);
    }
    throw new Error("Parameter [json] is Missing or is not an Object");
  };

  Queue.prototype._post = function(message, cb) {
    message = nullOnEmpty(message);
    if (message == null) {
      throw new Error("Parameter [message] is NOT a String or is an Empty String");
    }
    Message.__create(this.system, this.name, message, null, cb);
    return this;
  };

  Queue.prototype.find = function(id, cb) {
    id = validateMID(id);
    cb = _.isFunction(cb) ? cb : null;
    Message.__findInQueue(this.system, this.name, id, false, (function(_this) {
      return function(error, m) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        _this.emit("message-found", m);
        return typeof cb === "function" ? cb(null, m) : void 0;
      };
    })(this));
    return this;
  };

  Queue.prototype.exists = function(id, cb) {
    id = validateMID(id);
    cb = _.isFunction(cb) ? cb : null;
    Message.__findInQueue(this.system, this.name, id, false, (function(_this) {
      return function(error, m) {
        var exists;
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        exists = m != null ? true : false;
        _this.emit("message-exists", exists);
        return typeof cb === "function" ? cb(null, exists) : void 0;
      };
    })(this));
    return this;
  };

  Queue.prototype.pending = function(cb) {
    cb = _.isFunction(cb) ? cb : null;
    Queue.__pending(this.system, this.name, (function(_this) {
      return function(error, ids) {
        var list;
        if (typeof err !== "undefined" && err !== null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        list = ids != null ? ids : [];
        _this.emit("messages-pending", list);
        return typeof cb === "function" ? cb(null, list) : void 0;
      };
    })(this));
    return this;
  };

  Queue.prototype.peek = function(cb) {
    cb = _.isFunction(cb) ? cb : null;
    Queue.__pending(this.system, this.name, (function(_this) {
      return function(error, ids) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        if (ids.length) {
          return Message.__load(_this.system, ids[0], function(error, m) {
            if (error != null) {
              _this.emit("error", error);
              return typeof cb === "function" ? cb(error) : void 0;
            }
            _this.emit("message-peek", m);
            return typeof cb === "function" ? cb(null, m) : void 0;
          });
        } else {
          _this.emit("message-peek", null);
          return typeof cb === "function" ? cb(null, null) : void 0;
        }
      };
    })(this));
    return this;
  };

  Queue.prototype.receive = function(cb) {
    cb = _.isFunction(cb) ? cb : null;
    Queue.__pending(this.system, this.name, (function(_this) {
      return function(error, ids) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        if (ids.length) {
          return Message.__load(_this.system, ids[0], function(error, m) {
            var timeout;
            if (error != null) {
              _this.emit("error", error);
              return typeof cb === "function" ? cb(error) : void 0;
            }
            timeout = m._props.htimeout != null ? m._props.htimeout : _DEFAULTS.queue.htimeout;
            return Queue.__hideMessage(_this.system, _this.name, m.id(), timeout, function(error, ok) {
              if (error != null) {
                _this.emit("error", error);
                return typeof cb === "function" ? cb(error) : void 0;
              }
              m._props.hidden = true;
              _this.emit("message", m);
              return typeof cb === "function" ? cb(null, m) : void 0;
            });
          });
        } else {
          _this.emit("message", null);
          return typeof cb === "function" ? cb(null, null) : void 0;
        }
      };
    })(this));
    return this;
  };

  Queue.prototype.pop = function(cb) {
    cb = _.isFunction(cb) ? cb : null;
    Queue.__pending(this.system, this.name, (function(_this) {
      return function(error, ids) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        if (ids.length) {
          return Message.__load(_this.system, ids[0], function(error, m) {
            if (error != null) {
              _this.emit("error", error);
              return typeof cb === "function" ? cb(error) : void 0;
            }
            return m["delete"](function(error, m) {
              if (error != null) {
                _this.emit("error", error);
                return typeof cb === "function" ? cb(error) : void 0;
              }
              _this.emit("message-pop", m);
              return typeof cb === "function" ? cb(null, m) : void 0;
            });
          });
        } else {
          _this.emit("message-removed", null);
          return typeof cb === "function" ? cb(null, null) : void 0;
        }
      };
    })(this));
    return this;
  };

  return Queue;

})(EventEmitter);

System = (function(superClass) {
  extend(System, superClass);

  System.prototype._cache = {};

  System.getInstance = function(options, ns) {
    return new System(options, ns);
  };

  function System(options, _ns1) {
    var opts, ref;
    if (options == null) {
      options = {};
    }
    this._ns = _ns1;
    if (typeof _ns !== "undefined" && _ns !== null) {
      this._ns = _ns + ":";
    }
    if (((ref = options.constructor) != null ? ref.name : void 0) === "RedisClient") {
      this._redis = options;
    } else {
      opts = _.extend(_DEFAULTS.redis, options);
      this._redis = redis.createClient(opts.port, opts.host, opts.options);
    }
    this.connected = this._redis.connected || false;
    if (this.connected) {
      this.emit("connect", this);
      this.initializeRedis;
    }
    this._redis.on("connect", (function(_this) {
      return function() {
        _this.connected = true;
        _this.emit("connect", _this);
        return _this.initializeRedis;
      };
    })(this));
    this._redis.on("error", (function(_this) {
      return function(err) {
        if (err.message.indexOf("ECONNREFUSED")) {
          _this.connected = false;
          return _this.emit("disconnect");
        } else {
          console.error("Redis ERROR", err);
          return _this.emit("error", err);
        }
      };
    })(this));
  }

  System.prototype.quit = function(cb) {
    this._redis.quit((function(_this) {
      return function(err, resp) {
        if (err != null) {
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        _this.emit("disconnect", resp);
        return typeof cb === "function" ? cb(null, resp) : void 0;
      };
    })(this));
    return this;
  };

  System.prototype.post = function(message, queue, ifexists, cb) {
    var wrapper;
    if (ifexists == null) {
      ifexists = false;
    }
    message = nullOnEmpty(message);
    queue = validateQNAME(queue);
    if (message == null) {
      throw new Error("Parameter [message] is NOT a String or is an Empty String");
    }
    if (_.isFunction(ifexists)) {
      cb = ifexists;
      ifexists = false;
    } else {
      ifexists = !!ifexists;
    }
    cb = _.isFunction(cb) ? cb : null;
    wrapper = (function(_this) {
      return function(error, q) {
        if (typeof err !== "undefined" && err !== null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error, _this) : void 0;
        }
        return q._post(message, function(error, m) {
          if (error != null) {
            _this.emit("error", error);
            return typeof cb === "function" ? cb(error, _this) : void 0;
          }
          _this.emit("message-new", m, queue);
          return typeof cb === "function" ? cb(null, m) : void 0;
        });
      };
    })(this);
    if (ifexists) {
      Queue.__load(this, queue, wrapper);
    } else {
      Queue.__loadOrCreate(this, queue, _DEFAULTS.queue, wrapper);
    }
    return this;
  };

  System.prototype.postJSON = function(json, queue, ifexists, cb) {
    if (ifexists == null) {
      ifexists = false;
    }
    if (_.isPlainObject(json)) {
      return this.post(JSON.stringify(json), queue, ifexists, cb);
    }
    throw new Error("Parameter [json] is Missing or is not an Object");
  };

  System.prototype.queues = function(cb) {
    cb = _.isFunction(cb) ? cb : null;
    Queue.__list(this, (function(_this) {
      return function(error, list) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        _this.emit("queues", list);
        return typeof cb === "function" ? cb(null, list) : void 0;
      };
    })(this));
    return this;
  };

  System.prototype.queue = function(name, ifexists, options, cb) {
    if (_.isFunction(ifexists)) {
      cb = ifexists;
      ifexists = false;
      options = null;
    } else {
      ifexists = !!ifexists;
    }
    if (_.isFunction(options)) {
      cb = options;
      options = null;
    }
    if ((options != null) && !_.isPlainObject(options)) {
      throw new Error("Parameter [options] contains an Invalid Value");
    }
    cb = _.isFunction(cb) ? cb : null;
    return this._queue(name, false, options, cb);
  };

  System.prototype._queue = function(name, ifexists, options, cb) {
    var wrapper;
    name = validateQNAME(name);
    wrapper = (function(_this) {
      return function(error, q) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        _this.emit("queue", q);
        return typeof cb === "function" ? cb(null, q) : void 0;
      };
    })(this);
    if (ifexists) {
      Queue.__load(this, name, wrapper);
    } else {
      if (options != null) {
        options = _.extend((_DEFAULTS.queue, _.pick(options, function(value, key) {
          return ["htimeout", "etimeout", "plimit"].indexOf(key) > 0;
        })));
      } else {
        options = _DEFAULTS.queue;
      }
      Queue.__loadOrCreate(this, name, options, wrapper);
    }
    return this;
  };

  System.prototype.existsQueue = function(name, cb) {
    name = validateQNAME(name);
    cb = _.isFunction(cb) ? cb : null;
    Queue.__exists(this, name, (function(_this) {
      return function(error, exists) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        _this.emit("queue-exists", exists, name);
        return typeof cb === "function" ? cb(null, exists, name) : void 0;
      };
    })(this));
    return this;
  };

  System.prototype.find = function(id, name, active, cb) {
    var wrapper;
    if (name == null) {
      name = null;
    }
    if (active == null) {
      active = false;
    }
    if (_.isFunction(name)) {
      cb = name;
      active = false;
      name = null;
    } else if (_.isFunction(active)) {
      cb = name;
      active = false;
    }
    id = validateMID(id);
    name = nullOnEmpty(name);
    if (name != null) {
      name = validateQNAME(name);
    }
    cb = _.isFunction(cb) ? cb : null;
    wrapper = (function(_this) {
      return function(error, m) {
        if (error != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(error) : void 0;
        }
        _this.emit("found", m);
        return typeof cb === "function" ? cb(null, m) : void 0;
      };
    })(this);
    if (name != null) {
      Message.__findInQueue(this.system, this.name, id, false, wrapper);
    } else {
      Message.__findInAll(this.system, id, false, wrapper);
    }
    return this;
  };

  System.prototype._systemKey = function(names) {
    var key;
    if (_.isArray(names)) {
      key = names.join(":");
    } else if (_.isString(names)) {
      key = nullOnEmpty(names);
    } else {
      throw new Error("INVALID Parameter Value [names]");
    }
    if (this._ns != null) {
      key = this._ns + "key";
    }
    return key;
  };

  return System;

})(EventEmitter);

module.exports = System;
