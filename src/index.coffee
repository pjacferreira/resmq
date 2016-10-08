###
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
###

#
# DEPENDENCIES
#
redis = require "redis"
FlakeID = require "flake-idgen"
intFormat = require('biguint-format')
_ = require "lodash"
EventEmitter = require( "events" ).EventEmitter

#
# HELPER OBJECTS
#

_VALIDATORS =
  id:		/^([A-Z0-9:]){16}$/
  qname:	/^([a-zA-Z0-9_-]){1,160}$/

_DEFAULTS =
  redis:
    host: "127.0.0.1"
    port: 6379
    options:
      password: null    # No Password Authentication
      db: 0             # DEFAULT Database
  queue:
    htimeout: 30  # Hide Timeout Period (30s)
    etimeout: -1  # Message Expiry Period (-1) - Never Expiry
    plimit:   -1  # Message Process Limit (-1) - No Process Limit

#
# HELPER FUNCTIONS
#

# Return NULL if NOT String or is Empty String
nullOnEmpty = (string) ->
  if _.isString string
    string = string.trim()
    return if string.length then string
  return null

# Create a Unique ID for Messages
genID = () ->
  newID = new FlakeID
  intFormat(newID.next(), 'hex').toUpperCase()

# Validate if the Given Name is a Valid QUEUE Name
validateQNAME = (name) ->
  # Validate Queue Name
  name = nullOnEmpty name
  if not name?
    throw new Error "MISSING Queue Name [name]"
  else if not _VALIDATORS.qname.test name
    throw new Error "INVALID Queue Name [name]"
  return name

# Validate if the Given ID is a Valid Message ID
validateMID = (id) ->
  # Validate Message ID
  id = nullOnEmpty id
  if not name?
    throw new Error "MISSING Message ID [id]"
  else if not _VALIDATORS.id.test id
    throw new Error "INVALID Message ID [id]"
  return id

class Message extends EventEmitter
  @_redisProps = [
     "queue", "message", "hidden", "pcount",
     "htimeout", "etimeout", "plimit",
     "created", "modified"
  ]

  # Create a Message Instance (Allows for Method Chaining)
  @getInstance = (system) ->
    new Message system

  # Constructor
  constructor: (@system) ->

  post: (cb, queue, message, props = {}) ->
    try
      # Validate Message ID
      @_queue = validateQNAME queue

      # Genreate Message ID
      @_id = genID()

      # Cleanup Message Text
      @_message  = nullOnEmpty message
      if not message?
        throw new Error "INVALID Parameter Value [message]"

      # Default State Active
      @_hidden = false

      # Get REDIS Time
      redis = @system.server
      redis.time (err, rtime) =>
        if err? # HANDLE ERROR
          @emit "error", error
          return cb? err, @

        # Field<-->Value Pairs
        hmset = [ "HMSET", @system._systemKey ["messages", @_id] ]

        # Save Properties to Instance
        _.forOwn props, (value,key) =>
          switch key
            when "pcount"
              @_pcount = value
              hmset.push key, value
            when "htimeout"
              @_htimeout = value
              hmset.push key, value
            when "etimeout"
              @_etimeout = value
              hmset.push key, value
            when "plimit"
              @_plimit = value
              hmset.push key, value

        # Save Creation Modification Times to Instance
        @_created = rtime[0]
        @_modified = rtime[0]

        # Save Instance Properties to Redis Properties
        hmset.push "queue", @_queue
        hmset.push "message", @_message
        hmset.push "hidden", !!@_hidden
        hmset.push "created", rtime[0]
        hmset.push "modified", rtime[0]

        commands = [
          hmset
          [ "ZADD", @system._systemKey(["queue", @_queue, "M"]), @_created, @_id ]
          [ "HINCRBY", _this.system._systemKey(["queue", @_queue, "P"]), "received", 1 ]
        ]

        @system.server.multi(commands).exec (err, rcmds) =>
          if err? # HANDLE ERROR
            @emit "error", error
            return cb? err, @

          # Found a Valid Message
          @emit "new", @
          return cb? null, @

    catch error
      @emit "error", error
      cb? err, @

    return @

  find: (cb, id, qname, active = true) ->
    try
      # Validate Message ID
      id = validateMID id

      # Validate Queue NAME (if given)
      qname = qname ? validateQNAME qname

      # Try to Find the Message
      if qname?
        @_findInQueue(cb, id, queue, active)
      else
        @_findInAll(cb, id, active)

    catch error
      @emit "error", error
      cb? err, @

    return @

  # TODO: Test
  delete: (cb) ->
    try
      # Do we have the Minimum to Work With?
      validateMID @_id
      validateQNAME @_queue

      # Delete Message from System
      commands = [
        [ "ZREM",  _this.system._systemKey(["queue", @_queue, "M"]), @_id ]
        [ "ZREM",  _this.system._systemKey(["queue", @_queue, "H"]), @_id ]
        [ "DEL", _this.system._systemKey(["messages", @_id]) ]
      ]
      @system.server.multi(commands).exec (err, rcmds) =>
        if err? # HANDLE ERROR
          @emit "error", err
          return cb? err, null

        @emit "deleted", msg
        cb? null, msg

    catch err
      @emit "error", err
      cb? err, @

    return @

  # TODO: Test
  move: (toqueue, cb) ->
    try
      # Do we have the Minimum to Work With?
      validateMID @_id
      validateQNAME @_queue
      toqueue= validateQNAME toqueue

      # Is the destination queue different than the current?
      if @_queue != toqueue # YES
        redis = @system.server
        redis.time (err, rtime) =>
          if err? # HANDLE ERROR
            @emit "error", error
            return cb? err, @

          # Remove Message from Current Queue
          commands = [
            [ "ZREM",  _this.system._systemKey(["queue", @_queue, "M"]), @_id ]
            [ "ZREM",  _this.system._systemKey(["queue", @_queue, "H"]), @_id ]
          ]
          @system.server.multi(commands).exec (err, rcmds) =>
            if err? # HANDLE ERROR
              @emit "error", err
              return cb? err, null

            # Change the Messages Queue
            @_queue = toqueue
            # Unhide Message, if it was hidden
            @_hidden = false
            # Save Creation Modification Times to Instance
            @_modified = rtime[0]

            # HMSET Field<-->Value Pairs
            hmset = [
              "HMSET", @system._systemKey(["messages", @_id])
              "queue", @_queue
              "hidden", @_hidden
              "modified", rtime[0]
            ]

            commands = [
              hmset
              [ "ZADD", @system._systemKey(["queue", @_queue, "M"]), @_created, @_id ]
              [ "HINCRBY", _this.system._systemKey(["queue", @_queue, "P"]), "received", 1 ]
            ]

            @system.server.multi(commands).exec (err, rcmds) =>
              if err? # HANDLE ERROR
                @emit "error", error
                return cb? err, @

              # Message Moved
              @emit "moved", @
              cb? null, @

      else # NO: Nothing to do
        @emit "moved", @
        cb? null, @

    catch err
      @emit "error", err
      cb? err, @

    return @

  _findInAll: (cb, id, active) ->
    @__load id, (err, msg) =>
      if err? # HANDLE ERROR
        @emit "error", error
        return cb? err, msg

      # Do we have a Message in the Correct State?
      if msg? and (!active or !msg._hidden)  # YES
        # Found a Valid Message
        @emit "found", msg
        return cb? null, msg

      # ELSE: No Valid Message Found
      @emit "found"
      cb? null, null

    return @

  _findInQueue: (cb, id, queue)->
    @__load id, (err, msg) =>
      if err? # HANDLE ERROR
        @emit "error", error
        return cb? err, msg

      # Do we have a Message in the Correct Queue and State?
      if msg? and (msg._queue == queue) and (!active or !msg._hidden)  # YES
        # Found a Valid Message
        @emit "found", msg
        return cb? null, msg

      # ELSE: No Valid Message Found
      @emit "found"
      cb? null, null

    return @

  __load: (id, cb)->
    # Get Message Properties
    key = @system._systemKey ["messages", id]
    @system.server.hmget key, Message._redisProps, (err, resp) =>
      if err? # HANDLE ERROR
        return cb? err, @

      # Does the Message Exist?
      if resp.length
        # Extract Message Properties
        @_id       = id
        @_queue    = resp[0]
        @_message  = resp[1]
        @_hidden   = !!resp[2]
        @_pcount   = if resp[3]? then parseInt(resp[3], 10) else null
        @_htimeout = if resp[4]? then parseInt(resp[4], 10) else null
        @_etimeout = if resp[5]? then parseInt(resp[5], 10) else null
        @_plimit   = if resp[6]? then parseInt(resp[6], 10) else null
        @_created  = parseInt(resp[7], 10)
        @_modified = parseInt(resp[8], 10)

        # Found a Valid Message
        return cb? null, @

      # ELSE: No Valid Message Found
      cb? null, null

    return @

  __hide: (timeout, cb)=>
    # Get Current Redis Server TIME
    @system.server.time (err, rtime) =>
      if err? # HANDLE ERROR
        return cb? err, null

      timeout = rtime[0] + timeout
      commands = [
        [ "ZREM",  _this.system._systemKey(["queue", @_queue, "M"]), @_id ]
        [ "ZADD",  _this.system._systemKey(["queue", @_queue, "H"]), timeout, @_id ]
        [ "HSET", _this.system._systemKey(["messages", @_id]),      "hidden", 1 ]
      ]

      @system.server.multi(commands).exec (err, rcmds) =>
        if err? # HANDLE ERROR
          return cb? err, null

        @_hidden = true
        cb? null, _this


class Queue extends EventEmitter

  @_redisProps = [
     "htimeout", "etimeout", "plimit"
     "created", "modified", "received", "sent"
  ]

  # Create a Queue Instance (Allows for Method Chaining)
  @getInstance = (system,name) ->
    new Queue system, name

  # Constructor
  constructor: (@system, @name) ->

  # Refresh Queue Statistics
  refresh: (cb) ->
    @_load cb

  #############################
  # MESSAGE HANDLING FOR Queues
  #############################

  # Post Message to Queue
  post: (cb, message) ->
    msg = Message.getInstance @system

    msg
      .on "new", (msg) =>
        @emit "new-message", msg
      .on "error", (err) =>
        @emit "error", err
      .post cb, @name, message

    return @

  # See What is the Next Message at the Top of the Queue
  peek: (cb) ->
    # TODO: Handle Unhide
    # 1st : Unhide Messages Whose Visibility Timer Expired
    # i.e. search queue:messages:H for messages whose ZSCORE is less than or
    # equal to current server time
    # - TIME
    # - ZRANGEBYSCORE queue:messages:H -inf {server time}
    # - for each
    # Get 1st Element on Queue
    #redis.zrange key, 0, 0, (err, rtime) =>
    #  if err? # HANDLE ERROR
    #    return cb err, @

    # Get Current Redis Server TIME
    redis = @system.server
    redis.time (err, rtime) =>
      if err? # HANDLE ERROR
        @emit "error", err
        return cb? err, @

      key = @system._systemKey ["queue", @name, "M"]
      redis.zrangebyscore key, "-inf", rtime[0], (err, messages) =>
        if err? # HANDLE ERROR
          @emit "error", err
          return cb? err, @

        if messages.length
          msg = Message
                  .getInstance @system
                  .__load messages[0], (err, msg) =>
                    @emit "peek-message", msg
                    cb? null, msg

    # Return SELF
    return @

  # Receive Message from Queue (if any)
  receive: (cb) ->
    # Get Current Redis Server TIME
    redis = @system.server
    redis.time (err, rtime) =>
      if err? # HANDLE ERROR
        @emit "error", err
        return cb? err, @

      key = @system._systemKey ["queue", @name, "M"]
      redis.zrangebyscore key, "-inf", rtime[0], (err, messages) =>
        if err? # HANDLE ERROR
          @emit "error", err
          return cb? err, @

        # Do we have any messages pending?
        if messages.length # YES: Get 1st Message in Queue (FIFO Order)
          msg = Message.getInstance @system
          msg
            .__load messages[0], (err, msg) =>
              if err? # HANDLE ERROR
                @emit "error", err
                return cb? err, @

              # Does the Message have a Hide Timeout
              timeout = if msg?._htimeout? then msg._htimeout else _DEFAULTS.queue.htimeout
              if timeout > 0 # YES: Temporarily hide the Message
                msg.__hide timeout, (err, msg) =>
                  if err? # HANDLE ERROR
                    @emit "error", err
                    return cb? err, @

                  @emit "message", msg
                  cb? null, msg
              else # NO: Leave Message Visible in the Queue
                @emit "message", msg
                cb? null, msg
        else # NO: No Messages
          @emit "message", null

    # Return SELF
    return @

  # Pop Message from Queue (Read and Delete)
  pop: (cb) ->
    # Get Current Redis Server TIME
    redis = @system.server
    redis.time (err, rtime) =>
      if err? # HANDLE ERROR
        @emit "error", err
        return cb? err, @

      key = @system._systemKey ["queue", @name, "M"]
      redis.zrangebyscore key, "-inf", rtime[0], (err, messages) =>
        if err? # HANDLE ERROR
          @emit "error", err
          return cb? err, @

        # Do we have messages awating processing?
        if messages.length # YES
          msg = Message.getInstance @system
          msg
            .__load messages[0], (err, msg) =>
              if err? # HANDLE ERROR
                @emit "error", err
                return cb? err, null

              commands = [
                [ "ZREM",  _this.system._systemKey(["queue", @name, "M"]), msg._id ]
                [ "DEL", _this.system._systemKey(["messages", msg._id]) ]
                [ "HINCRBY", _this.system._systemKey(["queue", @name, "P"]), "sent", 1 ]
              ]

              @system.server.multi(commands).exec (err, rcmds) =>
                if err? # HANDLE ERROR
                  @emit "error", err
                  return cb? err, null

                @emit "message", msg
                @emit "removed-message", msg
                cb? null, msg
        else # NO: No Messages Waiting
          cb? null, null

    # Return SELF
    return @

  # Find the Message with the Given ID in the Current Queue
  inQueue: (cb, id, active = true) ->
    try
      # Validate Message ID
      id = validateMID id

      # Search Only Active Messages?
      if active # YES
        # Get Key Name
        key = @system._systemKey ["queue", @name, "M"]

        # Get Queue Properties
        @system.server.sismember key, id, (err, resp) =>
          if err? # HANDLE ERROR
            @emit "error", err
            return cb? err, @

          exists = !!resp
          @emit "exists", exists
          cb? null, exists

      else # NO: Search Active and Hidden
        commands = [
          [ "SISMEMBER", @system._systemKey ["queue", @name, "M"], id ]
          [ "SISMEMBER", @system._systemKey ["queue", @name, "H"], id ]
        ]

        @system.multi(commands).exec (err, results) =>
          if err? # HANDLE ERROR
            @emit "error", err
            return cb? err, @

          exists = !!(results[0] or results[1])
          @emit "exists", exists
          cb? null, exists

    catch err
      @emit "error", err
      cb? err, @

    return @


  # Find a Message in Current Queue
  #
  # @param [Function] (OPTIONAL) cb Callback Function, if passed will be called
  # @param [String] (REQUIRED) Message ID
  # @param [Boolean] (OPTIONAL: Default TRUE) Search for Active Messages Only?
  # @return [Object] self
  # @event error Any error generated during search
  # @event message-found Called if Message Found (message) or Not Found (null)
  find: (cb, id, active = true) ->
    msg = Message.getInstance @system

    msg
      .getInstance @system
      .on "found", (err) =>
        @emit "error", err
      .on "found", (msg) =>
        @emit "found-message", mss
      .find cb, id, @name, active

    return @

  # Does Queue Exist
  exists: (cb, name) ->
    try
      # Validate Queue Name
      name = validateQNAME name

      # Get Key Name
      key = @system._systemKey ["queue", name, "P"]

      @system.server.exists key, (err, resp) =>
        if err? # HANDLE ERROR
          @emit "error", err
          return cb? err, @

        exists = !!resp
        @emit "exists", exists
        cb? null, exists

    catch err
      @emit "error", err
      cb? err, @

    return @

  # Load Queues Properties from Redis
  load: (cb, name) ->
    try
      # Validate Queue Name
      name = validateQNAME name

      # Get Key Name
      key = @system._systemKey ["queue", name, "P"]
      @system.server.exists key, (err, resp) =>
        # Does the Queue Exist?
        if resp # YES
          @._load cb, name
        else
          # Call Callback
          @emit "not-found"
          cb? null, null

    catch err
      @emit "error", err
      cb? err, @

    return @

  _load: (cb, name) ->
    # Get Key Name
    key = @system._systemKey ["queue", name, "P"]

    # Get Queue Properties
    @system.server.hmget key, Queue._redisProps, (err, resp) =>
      if err? # HANDLE ERROR
        @emit "error", err
        return cb? err, @

      if resp.length
        # Extract Queue Properties
        @name = name
        @_htimeout= parseInt(resp[0], 10)
        @_etimeout= parseInt(resp[1], 10) or 0
        @_plimit= parseInt(resp[2], 10) or 0
        @_created= parseInt(resp[3], 10)
        @_modified= parseInt(resp[4], 10)
        @_received= parseInt(resp[5], 10)
        @_sent= parseInt(resp[6], 10)

        # Call Callback
        @emit "loaded", @
        cb? null, @
      else
        @emit "not-found", name
        cb? null, @

    return @

  # Create a New Queue with the Given Options
  create: (cb, name, options) ->
    try
      # Validate Queue Name
      name = validateQNAME name

      # Get Key Name
      key = @system._systemKey ["queue", name, "P"]
      @system.server.exists key, (err, resp) =>
        # Does the Queue Exist?
        if resp # YES: Error
          # Call Callback
          @emit "found"
          cb? null, null
        else # NO: Create It
          @._create cb, name, options

    catch err
      @emit "error", err
      cb? err, @

    return @

  # Create a New Queue with the Given Options
  _create: (cb, name, options) ->
    # Initialize Queue Options
    if !options?
      options = _DEFAULTS.queue
    else
      options = _.extend (
        _DEFAULTS.queue
        _.pick options, (value,key) ->
          ["htimeout", "etimeout", "plimit"].indexOf(key) > 0
        )

    # Get Current Redis Server TIME
    redis = @system.server
    redis.time (err, rtime) =>
      if err? # HANDLE ERROR
        @emit "error", err
        return cb? err, @

      # Commands to Set Queue Properties
      hmset = [
        "HMSET", @system._systemKey(["queue", name, "P"]),
        "htimeout", options.htimeout
        "etimeout", options.etimeout
        "plimit", options.plimit
        "created", rtime[0]
        "modified", rtime[0]
        "received", 0
        "sent", 0
      ]

      commands= [
        hmset
        [ "SADD", @system._systemKey("queues"), name ]
      ]

      redis.multi(commands).exec (err, rset) =>
        if err? # HANDLE ERROR
          @emit "error", err
          return cb? err, @

        # Save Properties
        @name      = name
        @_htimeout = options.htimeout
        @_etimeout = options.etimeout
        @_plimit   = options.plimit
        @_created  = rtime[0]
        @_modified = rtime[0]
        @_received = 0
        @_sent     = 0

        # Call Callback
        @emit "created", @
        cb? null, @

class System extends EventEmitter

  # Constructor
  constructor: (options = {}, @_ns) ->
    # Do we want a separate namespace for the System?
    @_ns = (_ns + ":") if _ns?

    # Did we receive a Redis Connection?
    if options.constructor?.name is "RedisClient" # YES
      @server = options
    else # NO: Create New Connection
      opts = _.extend _DEFAULTS.redis, options
      @server = redis.createClient(opts.port, opts.host, opts.options)

    # If external client is used it might already be connected. So we check here:
    @connected = @server.connected or false
    if @connected
      @emit "connect", @
      @initializeRedis

    # REDIS EVENTS: Connect
    @server.on "connect", =>
      @connected = true
      @emit "connect", @
      @initializeRedis

    # REDIS EVENTS: Error
    @server.on "error", (err) =>
      if err.message.indexOf "ECONNREFUSED"
        @connected = false
        @emit "disconnect"
      else
        console.error "Redis ERROR", err
        @emit "error", err

  quit: (cb) ->
    @server.quit (err, resp) =>
      if err? # HANDLE ERROR
        @emit "error", err
        return cb? err, @

      @emit "disconnect", resp
      cb? null, resp

    return @

  # List Queues in System
  queues: (cb) ->
    # REDIS : List Members of Set
    @server.smembers @_systemKey("queues"), (err, resp) =>
      if err? # HANDLE ERROR
        @emit "error", error
        return cb? err, @

      @emit "queues", resp
      cb? null, resp

    return @

  queue: (name, options, cb) ->
    # Was Options Set?
    if _.isFunction options # NO: it Actually the Callback
      cb = options
      options = null

    q = Queue.getInstance @
    q
      .on "created", (q) =>
        @emit "queue", q
        cb? null, q
      .on "loaded", (q) =>
        # TODO Apply Options is not null
        @emit "queue", q
        cb? null, q
      .on "not-found", (name) =>
        err = new  Error("Queue ["+name+"] not found")
        @emit "error", err
        cb? err, null
      .on "error", (err) =>
        @emit "error", err
        cb? err, null
      .on "exists", (exists) ->
        # NOTE: We use null as the callback, in order to avoid calling cb twice
        # when we process the events
        if exists
          q._load null, name
        else
          q._create null, name, options
      .exists null, name

  queueExists: (name, cb) ->
    Queue
      .getInstance @
      .exists cb, name
      .on "exists", (exists) =>
        @emit "queue-exists", name, exists
      .on "error", (err) =>
        @emit "error", err

    return @

  message: (name, cb) ->
    q = Queue.getInstance @
    q
      .on "message", (msg) =>
        @emit "message", msg
        cb? null, msg
      .on "loaded", (queue) ->
        q.receive
      .on "not-found", (name) =>
        err = new  Error("Queue ["+name+"] not found")
        @emit "error", err
        cb? err, null
      .on "error", (err) =>
        @emit "error", err
        cb? err, null
      ._load null, name

  # Find a Message Globally (in any Queue)
  #
  # @param [String] (REQUIRED) Message ID
  # @param [String] (OPTIONAL) Limit Search to Queue
  # @param [Boolean] (OPTIONAL: Default TRUE) Search for Active Messages Only?
  # @param [Function] (OPTIONAL) cb Callback Function, if passed will be called
  # @return [Object] self
  # @event error Any error generated during search
  # @event message-found Called if Message Found (message) or Not Found (null)
  findMessage: (id, name, active, cb) ->
    # Was Active Flag Set?
    if _.isFunction active # NO: it Actually the Callback
      cb = active
      active = true
    else  # Convert Active to Boolean
      active = !!active

    Message
      .getInstance @
      .find cb, id, name, active
      .on "found", (message) =>
        @emit "message-found", message
      .on "error", (err) =>
        @emit "error", err

    return @

  # Return a STRING to be used a REDIS Key in the System
  _systemKey: (names) ->
    if _.isArray names
      key = names.join ":"
    else if _.isString names
      key = nullOnEmpty names
    else
      throw new Error "INVALID Parameter Value [names]"

    key = "#{@_ns}key" if @_ns?
    return key

module.exports = System
