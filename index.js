
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
  bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
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
  if (typeof name === "undefined" || name === null) {
    throw new Error("MISSING Message ID [id]");
  } else if (!_VALIDATORS.id.test(id)) {
    throw new Error("INVALID Message ID [id]");
  }
  return id;
};

Message = (function(superClass) {
  extend(Message, superClass);

  Message._redisProps = ["queue", "message", "hidden", "pcount", "htimeout", "etimeout", "plimit", "created", "modified"];

  Message.getInstance = function(system) {
    return new Message(system);
  };

  function Message(system1) {
    this.system = system1;
    this.__hide = bind(this.__hide, this);
  }

  Message.prototype.post = function(cb, queue, message, props) {
    var error, error1;
    if (props == null) {
      props = {};
    }
    try {
      this._queue = validateQNAME(queue);
      this._id = genID();
      this._message = nullOnEmpty(message);
      if (message == null) {
        throw new Error("INVALID Parameter Value [message]");
      }
      this._hidden = false;
      redis = this.system.server;
      redis.time((function(_this) {
        return function(err, rtime) {
          var commands, hmset;
          if (err != null) {
            _this.emit("error", error);
            return typeof cb === "function" ? cb(err, _this) : void 0;
          }
          hmset = ["HMSET", _this.system._systemKey(["messages", _this._id])];
          _.forOwn(props, function(value, key) {
            switch (key) {
              case "pcount":
                _this._pcount = value;
                return hmset.push(key, value);
              case "htimeout":
                _this._htimeout = value;
                return hmset.push(key, value);
              case "etimeout":
                _this._etimeout = value;
                return hmset.push(key, value);
              case "plimit":
                _this._plimit = value;
                return hmset.push(key, value);
            }
          });
          _this._created = rtime[0];
          _this._modified = rtime[0];
          hmset.push("queue", _this._queue);
          hmset.push("message", _this._message);
          hmset.push("hidden", !!_this._hidden);
          hmset.push("created", rtime[0]);
          hmset.push("modified", rtime[0]);
          commands = [hmset, ["ZADD", _this.system._systemKey(["queue", _this._queue, "M"]), _this._created, _this._id], ["HINCRBY", _this.system._systemKey(["queue", _this._queue, "P"]), "received", 1]];
          return _this.system.server.multi(commands).exec(function(err, rcmds) {
            if (err != null) {
              _this.emit("error", error);
              return typeof cb === "function" ? cb(err, _this) : void 0;
            }
            _this.emit("new", _this);
            return typeof cb === "function" ? cb(null, _this) : void 0;
          });
        };
      })(this));
    } catch (error1) {
      error = error1;
      this.emit("error", error);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Message.prototype.find = function(cb, id, qname, active) {
    var error, error1;
    if (active == null) {
      active = true;
    }
    try {
      id = validateMID(id);
      qname = qname != null ? qname : validateQNAME(qname);
      if (qname != null) {
        this._findInQueue(cb, id, queue, active);
      } else {
        this._findInAll(cb, id, active);
      }
    } catch (error1) {
      error = error1;
      this.emit("error", error);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Message.prototype["delete"] = function(cb) {
    var commands, err, error1;
    try {
      validateMID(this._id);
      validateQNAME(this._queue);
      commands = [["ZREM", _this.system._systemKey(["queue", this._queue, "M"]), this._id], ["ZREM", _this.system._systemKey(["queue", this._queue, "H"]), this._id], ["DEL", _this.system._systemKey(["messages", this._id])]];
      this.system.server.multi(commands).exec((function(_this) {
        return function(err, rcmds) {
          if (err != null) {
            _this.emit("error", err);
            return typeof cb === "function" ? cb(err, null) : void 0;
          }
          _this.emit("deleted", msg);
          return typeof cb === "function" ? cb(null, msg) : void 0;
        };
      })(this));
    } catch (error1) {
      err = error1;
      this.emit("error", err);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Message.prototype.move = function(toqueue, cb) {
    var err, error1;
    try {
      validateMID(this._id);
      validateQNAME(this._queue);
      toqueue = validateQNAME(toqueue);
      if (this._queue !== toqueue) {
        redis = this.system.server;
        redis.time((function(_this) {
          return function(err, rtime) {
            var commands;
            if (err != null) {
              _this.emit("error", error);
              return typeof cb === "function" ? cb(err, _this) : void 0;
            }
            commands = [["ZREM", _this.system._systemKey(["queue", _this._queue, "M"]), _this._id], ["ZREM", _this.system._systemKey(["queue", _this._queue, "H"]), _this._id]];
            return _this.system.server.multi(commands).exec(function(err, rcmds) {
              var hmset;
              if (err != null) {
                _this.emit("error", err);
                return typeof cb === "function" ? cb(err, null) : void 0;
              }
              _this._queue = toqueue;
              _this._hidden = false;
              _this._modified = rtime[0];
              hmset = ["HMSET", _this.system._systemKey(["messages", _this._id]), "queue", _this._queue, "hidden", _this._hidden, "modified", rtime[0]];
              commands = [hmset, ["ZADD", _this.system._systemKey(["queue", _this._queue, "M"]), _this._created, _this._id], ["HINCRBY", _this.system._systemKey(["queue", _this._queue, "P"]), "received", 1]];
              return _this.system.server.multi(commands).exec(function(err, rcmds) {
                if (err != null) {
                  _this.emit("error", error);
                  return typeof cb === "function" ? cb(err, _this) : void 0;
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
    } catch (error1) {
      err = error1;
      this.emit("error", err);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Message.prototype._findInAll = function(cb, id, active) {
    this.__load(id, (function(_this) {
      return function(err, msg) {
        if (err != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(err, msg) : void 0;
        }
        if ((msg != null) && (!active || !msg._hidden)) {
          _this.emit("found", msg);
          return typeof cb === "function" ? cb(null, msg) : void 0;
        }
        _this.emit("found");
        return typeof cb === "function" ? cb(null, null) : void 0;
      };
    })(this));
    return this;
  };

  Message.prototype._findInQueue = function(cb, id, queue) {
    this.__load(id, (function(_this) {
      return function(err, msg) {
        if (err != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(err, msg) : void 0;
        }
        if ((msg != null) && (msg._queue === queue) && (!active || !msg._hidden)) {
          _this.emit("found", msg);
          return typeof cb === "function" ? cb(null, msg) : void 0;
        }
        _this.emit("found");
        return typeof cb === "function" ? cb(null, null) : void 0;
      };
    })(this));
    return this;
  };

  Message.prototype.__load = function(id, cb) {
    var key;
    key = this.system._systemKey(["messages", id]);
    this.system.server.hmget(key, Message._redisProps, (function(_this) {
      return function(err, resp) {
        if (err != null) {
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        if (resp.length) {
          _this._id = id;
          _this._queue = resp[0];
          _this._message = resp[1];
          _this._hidden = !!resp[2];
          _this._pcount = resp[3] != null ? parseInt(resp[3], 10) : null;
          _this._htimeout = resp[4] != null ? parseInt(resp[4], 10) : null;
          _this._etimeout = resp[5] != null ? parseInt(resp[5], 10) : null;
          _this._plimit = resp[6] != null ? parseInt(resp[6], 10) : null;
          _this._created = parseInt(resp[7], 10);
          _this._modified = parseInt(resp[8], 10);
          return typeof cb === "function" ? cb(null, _this) : void 0;
        }
        return typeof cb === "function" ? cb(null, null) : void 0;
      };
    })(this));
    return this;
  };

  Message.prototype.__hide = function(timeout, cb) {
    return this.system.server.time((function(_this) {
      return function(err, rtime) {
        var commands;
        if (err != null) {
          return typeof cb === "function" ? cb(err, null) : void 0;
        }
        timeout = rtime[0] + timeout;
        commands = [["ZREM", _this.system._systemKey(["queue", _this._queue, "M"]), _this._id], ["ZADD", _this.system._systemKey(["queue", _this._queue, "H"]), timeout, _this._id], ["HSET", _this.system._systemKey(["messages", _this._id]), "hidden", 1]];
        return _this.system.server.multi(commands).exec(function(err, rcmds) {
          if (err != null) {
            return typeof cb === "function" ? cb(err, null) : void 0;
          }
          _this._hidden = true;
          return typeof cb === "function" ? cb(null, _this) : void 0;
        });
      };
    })(this));
  };

  return Message;

})(EventEmitter);

Queue = (function(superClass) {
  extend(Queue, superClass);

  Queue._redisProps = ["htimeout", "etimeout", "plimit", "created", "modified", "received", "sent"];

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

  Queue.prototype.post = function(cb, message) {
    var msg;
    msg = Message.getInstance(this.system);
    msg.on("new", (function(_this) {
      return function(msg) {
        return _this.emit("new-message", msg);
      };
    })(this)).on("error", (function(_this) {
      return function(err) {
        return _this.emit("error", err);
      };
    })(this)).post(cb, this.name, message);
    return this;
  };

  Queue.prototype.peek = function(cb) {
    redis = this.system.server;
    redis.time((function(_this) {
      return function(err, rtime) {
        var key;
        if (err != null) {
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        key = _this.system._systemKey(["queue", _this.name, "M"]);
        return redis.zrangebyscore(key, "-inf", rtime[0], function(err, messages) {
          var msg;
          if (err != null) {
            _this.emit("error", err);
            return typeof cb === "function" ? cb(err, _this) : void 0;
          }
          if (messages.length) {
            return msg = Message.getInstance(_this.system).__load(messages[0], function(err, msg) {
              _this.emit("peek-message", msg);
              return typeof cb === "function" ? cb(null, msg) : void 0;
            });
          }
        });
      };
    })(this));
    return this;
  };

  Queue.prototype.receive = function(cb) {
    redis = this.system.server;
    redis.time((function(_this) {
      return function(err, rtime) {
        var key;
        if (err != null) {
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        key = _this.system._systemKey(["queue", _this.name, "M"]);
        return redis.zrangebyscore(key, "-inf", rtime[0], function(err, messages) {
          var msg;
          if (err != null) {
            _this.emit("error", err);
            return typeof cb === "function" ? cb(err, _this) : void 0;
          }
          if (messages.length) {
            msg = Message.getInstance(_this.system);
            return msg.__load(messages[0], function(err, msg) {
              var timeout;
              if (err != null) {
                _this.emit("error", err);
                return typeof cb === "function" ? cb(err, _this) : void 0;
              }
              timeout = (msg != null ? msg._htimeout : void 0) != null ? msg._htimeout : _DEFAULTS.queue.htimeout;
              if (timeout > 0) {
                return msg.__hide(timeout, function(err, msg) {
                  if (err != null) {
                    _this.emit("error", err);
                    return typeof cb === "function" ? cb(err, _this) : void 0;
                  }
                  _this.emit("message", msg);
                  return typeof cb === "function" ? cb(null, msg) : void 0;
                });
              } else {
                _this.emit("message", msg);
                return typeof cb === "function" ? cb(null, msg) : void 0;
              }
            });
          } else {
            _this.emit("message", null);
            return typeof cb === "function" ? cb(null, null) : void 0;
          }
        });
      };
    })(this));
    return this;
  };

  Queue.prototype.pop = function(cb) {
    redis = this.system.server;
    redis.time((function(_this) {
      return function(err, rtime) {
        var key;
        if (err != null) {
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        key = _this.system._systemKey(["queue", _this.name, "M"]);
        return redis.zrangebyscore(key, "-inf", rtime[0], function(err, messages) {
          var msg;
          if (err != null) {
            _this.emit("error", err);
            return typeof cb === "function" ? cb(err, _this) : void 0;
          }
          if (messages.length) {
            msg = Message.getInstance(_this.system);
            return msg.__load(messages[0], function(err, msg) {
              var commands;
              if (err != null) {
                _this.emit("error", err);
                return typeof cb === "function" ? cb(err, null) : void 0;
              }
              commands = [["ZREM", _this.system._systemKey(["queue", _this.name, "M"]), msg._id], ["DEL", _this.system._systemKey(["messages", msg._id])], ["HINCRBY", _this.system._systemKey(["queue", _this.name, "P"]), "sent", 1]];
              return _this.system.server.multi(commands).exec(function(err, rcmds) {
                if (err != null) {
                  _this.emit("error", err);
                  return typeof cb === "function" ? cb(err, null) : void 0;
                }
                _this.emit("message", msg);
                _this.emit("removed-message", msg);
                return typeof cb === "function" ? cb(null, msg) : void 0;
              });
            });
          } else {
            return typeof cb === "function" ? cb(null, null) : void 0;
          }
        });
      };
    })(this));
    return this;
  };

  Queue.prototype.inQueue = function(cb, id, active) {
    var commands, err, error1, key;
    if (active == null) {
      active = true;
    }
    try {
      id = validateMID(id);
      if (active) {
        key = this.system._systemKey(["queue", this.name, "M"]);
        this.system.server.sismember(key, id, (function(_this) {
          return function(err, resp) {
            var exists;
            if (err != null) {
              _this.emit("error", err);
              return typeof cb === "function" ? cb(err, _this) : void 0;
            }
            exists = !!resp;
            _this.emit("exists", exists);
            return typeof cb === "function" ? cb(null, exists) : void 0;
          };
        })(this));
      } else {
        commands = [["SISMEMBER", this.system._systemKey(["queue", this.name, "M"], id)], ["SISMEMBER", this.system._systemKey(["queue", this.name, "H"], id)]];
        this.system.multi(commands).exec((function(_this) {
          return function(err, results) {
            var exists;
            if (err != null) {
              _this.emit("error", err);
              return typeof cb === "function" ? cb(err, _this) : void 0;
            }
            exists = !!(results[0] || results[1]);
            _this.emit("exists", exists);
            return typeof cb === "function" ? cb(null, exists) : void 0;
          };
        })(this));
      }
    } catch (error1) {
      err = error1;
      this.emit("error", err);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Queue.prototype.find = function(cb, id, active) {
    var msg;
    if (active == null) {
      active = true;
    }
    msg = Message.getInstance(this.system);
    msg.getInstance(this.system).on("found", (function(_this) {
      return function(err) {
        return _this.emit("error", err);
      };
    })(this)).on("found", (function(_this) {
      return function(msg) {
        return _this.emit("found-message", mss);
      };
    })(this)).find(cb, id, this.name, active);
    return this;
  };

  Queue.prototype.exists = function(cb, name) {
    var err, error1, key;
    try {
      name = validateQNAME(name);
      key = this.system._systemKey(["queue", name, "P"]);
      this.system.server.exists(key, (function(_this) {
        return function(err, resp) {
          var exists;
          if (err != null) {
            _this.emit("error", err);
            return typeof cb === "function" ? cb(err, _this) : void 0;
          }
          exists = !!resp;
          _this.emit("exists", exists);
          return typeof cb === "function" ? cb(null, exists) : void 0;
        };
      })(this));
    } catch (error1) {
      err = error1;
      this.emit("error", err);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Queue.prototype.load = function(cb, name) {
    var err, error1, key;
    try {
      name = validateQNAME(name);
      key = this.system._systemKey(["queue", name, "P"]);
      this.system.server.exists(key, (function(_this) {
        return function(err, resp) {
          if (resp) {
            return _this._load(cb, name);
          } else {
            _this.emit("not-found");
            return typeof cb === "function" ? cb(null, null) : void 0;
          }
        };
      })(this));
    } catch (error1) {
      err = error1;
      this.emit("error", err);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Queue.prototype._load = function(cb, name) {
    var key;
    key = this.system._systemKey(["queue", name, "P"]);
    this.system.server.hmget(key, Queue._redisProps, (function(_this) {
      return function(err, resp) {
        if (err != null) {
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        if (resp.length) {
          _this.name = name;
          _this._htimeout = parseInt(resp[0], 10);
          _this._etimeout = parseInt(resp[1], 10) || 0;
          _this._plimit = parseInt(resp[2], 10) || 0;
          _this._created = parseInt(resp[3], 10);
          _this._modified = parseInt(resp[4], 10);
          _this._received = parseInt(resp[5], 10);
          _this._sent = parseInt(resp[6], 10);
          _this.emit("loaded", _this);
          return typeof cb === "function" ? cb(null, _this) : void 0;
        } else {
          _this.emit("not-found", name);
          return typeof cb === "function" ? cb(null, _this) : void 0;
        }
      };
    })(this));
    return this;
  };

  Queue.prototype.create = function(cb, name, options) {
    var err, error1, key;
    try {
      name = validateQNAME(name);
      key = this.system._systemKey(["queue", name, "P"]);
      this.system.server.exists(key, (function(_this) {
        return function(err, resp) {
          if (resp) {
            _this.emit("found");
            return typeof cb === "function" ? cb(null, null) : void 0;
          } else {
            return _this._create(cb, name, options);
          }
        };
      })(this));
    } catch (error1) {
      err = error1;
      this.emit("error", err);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  Queue.prototype._create = function(cb, name, options) {
    if (options == null) {
      options = _DEFAULTS.queue;
    } else {
      options = _.extend((_DEFAULTS.queue, _.pick(options, function(value, key) {
        return ["htimeout", "etimeout", "plimit"].indexOf(key) > 0;
      })));
    }
    redis = this.system.server;
    return redis.time((function(_this) {
      return function(err, rtime) {
        var commands, hmset;
        if (err != null) {
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        hmset = ["HMSET", _this.system._systemKey(["queue", name, "P"]), "htimeout", options.htimeout, "etimeout", options.etimeout, "plimit", options.plimit, "created", rtime[0], "modified", rtime[0], "received", 0, "sent", 0];
        commands = [hmset, ["SADD", _this.system._systemKey("queues"), name]];
        return redis.multi(commands).exec(function(err, rset) {
          if (err != null) {
            _this.emit("error", err);
            return typeof cb === "function" ? cb(err, _this) : void 0;
          }
          _this.name = name;
          _this._htimeout = options.htimeout;
          _this._etimeout = options.etimeout;
          _this._plimit = options.plimit;
          _this._created = rtime[0];
          _this._modified = rtime[0];
          _this._received = 0;
          _this._sent = 0;
          _this.emit("created", _this);
          return typeof cb === "function" ? cb(null, _this) : void 0;
        });
      };
    })(this));
  };

  return Queue;

})(EventEmitter);

System = (function(superClass) {
  extend(System, superClass);

  System.prototype._cache = {};

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
      this.server = options;
    } else {
      opts = _.extend(_DEFAULTS.redis, options);
      this.server = redis.createClient(opts.port, opts.host, opts.options);
    }
    this.connected = this.server.connected || false;
    if (this.connected) {
      this.emit("connect", this);
      this.initializeRedis;
    }
    this.server.on("connect", (function(_this) {
      return function() {
        _this.connected = true;
        _this.emit("connect", _this);
        return _this.initializeRedis;
      };
    })(this));
    this.server.on("error", (function(_this) {
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
    this.server.quit((function(_this) {
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

  System.prototype.queues = function(cb) {
    this.server.smembers(this._systemKey("queues"), (function(_this) {
      return function(err, resp) {
        if (err != null) {
          _this.emit("error", error);
          return typeof cb === "function" ? cb(err, _this) : void 0;
        }
        _this.emit("queues", resp);
        return typeof cb === "function" ? cb(null, resp) : void 0;
      };
    })(this));
    return this;
  };

  System.prototype.queue = function(name, options, cb) {
    var err, error1, q;
    if (_.isFunction(options)) {
      cb = options;
      options = null;
    }
    try {
      name = validateQNAME(name);
      if (this._cache.hasOwnProperty(name)) {
        this.emit("queue", q);
        if (typeof cb === "function") {
          cb(null, q);
        }
        return this;
      }
      q = Queue.getInstance(this);
      q.on("created", (function(_this) {
        return function(q) {
          _this._cache[name] = q;
          _this.emit("queue", q);
          return typeof cb === "function" ? cb(null, q) : void 0;
        };
      })(this)).on("loaded", (function(_this) {
        return function(q) {
          _this._cache[name] = q;
          _this.emit("queue", q);
          return typeof cb === "function" ? cb(null, q) : void 0;
        };
      })(this)).on("not-found", (function(_this) {
        return function(name) {
          var err;
          err = new Error("Queue [" + name + "] not found");
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, null) : void 0;
        };
      })(this)).on("error", (function(_this) {
        return function(err) {
          _this.emit("error", err);
          return typeof cb === "function" ? cb(err, null) : void 0;
        };
      })(this)).on("exists", function(exists) {
        if (exists) {
          return q._load(null, name);
        } else {
          return q._create(null, name, options);
        }
      }).exists(null, name);
    } catch (error1) {
      err = error1;
      this.emit("error", err);
      if (typeof cb === "function") {
        cb(err, this);
      }
    }
    return this;
  };

  System.prototype.queueExists = function(name, cb) {
    Queue.getInstance(this).exists(cb, name).on("exists", (function(_this) {
      return function(exists) {
        return _this.emit("queue-exists", name, exists);
      };
    })(this)).on("error", (function(_this) {
      return function(err) {
        return _this.emit("error", err);
      };
    })(this));
    return this;
  };

  System.prototype.message = function(name, cb) {
    var q;
    q = Queue.getInstance(this);
    return q.on("message", (function(_this) {
      return function(msg) {
        _this.emit("message", msg);
        return typeof cb === "function" ? cb(null, msg) : void 0;
      };
    })(this)).on("loaded", function(queue) {
      return q.receive;
    }).on("not-found", (function(_this) {
      return function(name) {
        var err;
        err = new Error("Queue [" + name + "] not found");
        _this.emit("error", err);
        return typeof cb === "function" ? cb(err, null) : void 0;
      };
    })(this)).on("error", (function(_this) {
      return function(err) {
        _this.emit("error", err);
        return typeof cb === "function" ? cb(err, null) : void 0;
      };
    })(this))._load(null, name);
  };

  System.prototype.findMessage = function(id, name, active, cb) {
    if (_.isFunction(active)) {
      cb = active;
      active = true;
    } else {
      active = !!active;
    }
    Message.getInstance(this).find(cb, id, name, active).on("found", (function(_this) {
      return function(message) {
        return _this.emit("message-found", message);
      };
    })(this)).on("error", (function(_this) {
      return function(err) {
        return _this.emit("error", err);
      };
    })(this));
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
