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
  if not id?
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

  @__load: (system, id, cb)->
    # Get Message Properties
    key = system._systemKey ["messages", id]

    # REDIS: Get Message Properties
    system._redis.hmget key, Message._redisProps, (error, resp) ->
      if error? # HANDLE ERROR
        return cb error

      # Does the Message Exist?
      if resp.length
        # Create Queue Instance
        m = Message.getInstance system

        # Extract Message Properties
        m._id                = id
        m._props["queue"]    = resp[0]
        m._props["message"]  = resp[1]
        m._props["hidden"]   = !!resp[2]
        m._props["pcount"]   = if resp[3]? then parseInt(resp[3], 10) else null
        m._props["htimeout"] = if resp[4]? then parseInt(resp[4], 10) else null
        m._props["etimeout"] = if resp[5]? then parseInt(resp[5], 10) else null
        m._props["plimit"]   = if resp[6]? then parseInt(resp[6], 10) else null
        m._props["created"]  = parseInt(resp[7], 10)
        m._props["modified"] = parseInt(resp[8], 10)

        # Found a Valid Message
        return cb null, m

      # ELSE: No Valid Message Found
      cb null, null

  @__create: (system, queue, message, props = {}, cb) ->
    redis = system._redis

    # Get REDIS Time
    redis.time (err, rtime) ->
      if error? # HANDLE ERROR
        return cb error, null

      # Generate Message ID
      id = genID()

      # Field<-->Value Pairs
      hmset = [ "HMSET", system._systemKey ["messages", id] ]

      # Message Properties
      props = { hidden: false }
      hmset.push "hidden", false

      # Transfer Known Properties to HMSET
      _.forOwn props, (value,key) ->
        if _.indexOf(Message._redisProps, key) >= 0
          hmset.push key, props[key] = value

      # Creation Time : (UNIX TIMESTAMP)+SERVER MICROSENDOS
      created = parseInt(rtime[0], 10)*1000000+parseInt(rtime[1], 10)

      # Save Instance Properties to Redis Properties
      hmset.push "queue", props["queue"] = queue
      hmset.push "message", props["message"] = message
      hmset.push "created", props["created"] = created
      hmset.push "modified", props["modified"] = created

      commands = [
        hmset
        [ "ZADD", system._systemKey(["queue", queue, "M"]), created, id ]
        [ "HINCRBY", system._systemKey(["queue", queue, "P"]), "received", 1 ]
      ]

      redis.multi(commands).exec (error, rcmds) ->
        if error? # HANDLE ERROR
          return cb error, null

        # Create Message Instance
        m = Message.getInstance system

        # Extract Message Properties
        m._id       = id
        m._props    = props

        # Found a Valid Message
        return cb null, m

  @__delete: (system, queue, id, cb) ->
    redis = system._redis

    commands = [
      [ "ZREM",  system._systemKey(["queue", queue, "M"]), id ]
      [ "ZREM",  system._systemKey(["queue", queue, "H"]), id ]
      [ "DEL",   system._systemKey(["messages", id]) ]
    ]

    redis.multi(commands).exec (error, rcmds) ->
      if error? # HANDLE ERROR
        return cb error

      cb null, id

  @__findInAll: (system, id, active, cb) ->
    Message.__load system, id, (error, m) ->
      if error? # HANDLE ERROR
        return cb error

      # Do we have a Message in the Correct State?
      if m? and (!active or !m._props.hidden)  # YES
        # Found a Valid Message
        return cb null, msg
      # ELSE: No Valid Message Found
      cb null, null

  @__updateProperty: (system, id, pname, pvalue, cb) ->
    # REDIS: Set Message Properties
    key = system._systemKey(["messages", id])
    system._redis.hmset key, pname, pvalue, (error, resp) ->
      if error? # HANDLE ERROR
        cb error

      return cb null, true

  # Message ID
  _id: null

  # REDIS Message Properties
  _props: {}

  # Create a Message Instance (Allows for Method Chaining)
  @getInstance = (system) ->
    new Message system

  # Constructor
  constructor: (@system) ->

  id: ->
    return @_id

  message: ->
    return @_props.message

  queue: ->
    return @_props.queue


  update: (message, cb) ->
    # Validate 'message' Parameter
    message = nullOnEmpty message
    if !message?
      throw new Error("Parameter [message] is NOT a String or is an Empty String")

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Message.__updateProperty @system, @_id, "message", message, (error, ok) =>
      # HANDLE ERROR
      if error?
        @emit "error", error
        return cb? error, @

      if ok
        # Update Message Parameter
        old = @_props["message"]
        @_props["message"] = message

        @emit "updated", @, "message", old
        return cb? error, @, "message", old
      else
        error = new Error "Failed to Update [message] in Message ["+@_id+"]"
        @emit "error", error
        return cb? error, @

    return @

  updateJSON: (json, cb) ->
    # Validate 'json' Parameter
    if _.isPlainObject json
      return @update JSON.stringify(json), cb

    throw new Error("Parameter [json] is Missing or is not an Object")

  delete: (cb) ->

    Message.__delete @system, @_props.queue, @_id, (error, id) =>
      if error? # HANDLE ERROR
        @emit "error", error
        cb? error

      @emit "deleted", @
      cb? null, @

    return @

  move: (toqueue, cb) ->
    # Do we have the Minimum to Work With?
    toqueue= validateQNAME toqueue

    # Is the destination queue different than the current?
    if @_props.queue != toqueue # YES

      Queue.__exists @system, toqueue, (error, exists) =>
        if error? # HANDLE ERROR
          @emit "error", error
          cb? error

        if !exists
          error = new Error "Destination queue ["+toqueue+"] does not exist"
          @emit "error", error
          cb? error

        Queue.__removeMessage @system, @_props.queue, @_id, (error, id) =>
          if error? # HANDLE ERROR
            @emit "error", error
            cb? error

          Queue.__addMessage @system, toqueue, @_id, (error, id) =>
            if error? # HANDLE ERROR
              @emit "error", error
              cb? error

            # Message Moved
            @emit "moved", @
            cb? null, @

    else # NO: Nothing to do
      @emit "moved", @
      cb? null, @

    return @

  hide: (toqueue, cb) ->
    # Does the Message have a Hide Timeout
    timeout = if @_props.htimeout? then @_props.htimeout else _DEFAULTS.queue.htimeout

    # TODO: Handle Situation in Which Message Doesn't have a hide timeout, BUT
    # the Queue has non default timeout
    Queue.__hideMessage @system, @_props.queue, @_id, timeout, (error, hidden) =>
      if error? # HANDLE ERROR
        @emit "error", error
        cb? error

      # Message Hidden
      @emit "hidden", @
      cb? null, @

    return @

  refresh: (cb) ->
    # Get Message Properties
    key = @system._systemKey ["messages", @_id]

    # REDIS: Get Message Properties
    @system._redis.hmget key, Message._redisProps, (error, resp) =>
      if error? # HANDLE ERROR
        @emit "error", error
        cb? error

      # Does the Message Exist?
      if resp.length
        # Extract Message Properties
        @_props["message"]  = resp[1]
        @_props["hidden"]   = !!resp[2]
        @_props["pcount"]   = if resp[3]? then parseInt(resp[3], 10) else null
        @_props["htimeout"] = if resp[4]? then parseInt(resp[4], 10) else null
        @_props["etimeout"] = if resp[5]? then parseInt(resp[5], 10) else null
        @_props["plimit"]   = if resp[6]? then parseInt(resp[6], 10) else null
        @_props["created"]  = parseInt(resp[7], 10)
        @_props["modified"] = parseInt(resp[8], 10)

        # Found a Valid Message
        return cb null, @

      # ELSE: No Valid Message Found !?
      error = new Error "Invalid Message [" + @_id + "]"
      @emit "error", error
      cb? error

    return @

class Queue extends EventEmitter

  @_redisProps = [
     "htimeout", "etimeout", "plimit"
     "created", "modified", "received", "sent"
  ]

  # Does Queue Exist
  @__list: (system, cb) ->
    # REDIS : List Members of Set
    system._redis.smembers system._systemKey("queues"), (error, resp) ->
      if error? # HANDLE ERROR
        return cb error

      # TODO: Return Empty Array if Response is Null?
      cb null, resp

  @__loadOrCreate: (system, name, options, cb) ->

    Queue.__exists system, name, (error, exists) ->
      if error? # HANDLE ERROR
        return cb error, null

      if exists
        Queue.__load system, name, cb
      else
        Queue.__create system, name, options, cb

  # Does Queue Exist
  @__exists: (system, name, cb) ->
    # Get Key Name
    key = system._systemKey ["queue", name, "P"]

    # REDIS: Does Key Exist?
    system._redis.exists key, (err, resp) ->
      if err? # HANDLE ERROR
        return cb err, null

      exists = !!resp
      cb null, exists

    return @

  @__load: (system, name, cb) ->
    # Get Key Name
    key = system._systemKey ["queue", name, "P"]

    # REDIS: Get Queue Properties
    system._redis.hmget key, Queue._redisProps, (error, resp) ->
      if error? # HANDLE ERROR
        return cb error

      if resp.length
        # Create Queue Instance
        q = Queue.getInstance system, name

        # Set Queue Properties
        q._htimeout= parseInt(resp[0], 10)
        q._etimeout= parseInt(resp[1], 10) or 0
        q._plimit= parseInt(resp[2], 10) or 0
        q._created= parseInt(resp[3], 10)
        q._modified= parseInt(resp[4], 10)
        q._received= parseInt(resp[5], 10)
        q._sent= parseInt(resp[6], 10)

        # Call Callback
        cb null, q
      else
        error = new Error("Queue ["+name+"] does not exist")
        cb error

  # Create a New Queue with the Given Options
  @__create: (system, name, options, cb) ->
    # REDIS: Get Current Server TIME
    system._redis.time (err, rtime) ->
      if err? # HANDLE ERROR
        return cb err, null

      # Commands to Set Queue Properties
      hmset = [
        "HMSET", system._systemKey(["queue", name, "P"]),
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
        [ "SADD", system._systemKey("queues"), name ]
      ]

      # REDIS: Create Queue
      system._redis.multi(commands).exec (err, rset) ->
        if err? # HANDLE ERROR
          return cb err

        # Create Queue Instance
        q = Queue.getInstance system, name

        # Save Properties
        q._htimeout = options.htimeout
        q._etimeout = options.etimeout
        q._plimit   = options.plimit
        q._created  = rtime[0]
        q._modified = rtime[0]
        q._received = 0
        q._sent     = 0

        cb null, q

  @__addMessage: (system, queue, id, cb) ->
    redis = system._redis

    # Get Current Redis Server TIME
    redis.time (error, rtime) ->
      if error? # HANDLE ERROR
        return cb error

      # Modification Time : (UNIX TIMESTAMP)+SERVER MICROSENDOS
      modified = parseInt(rtime[0], 10)*1000000+parseInt(rtime[1], 10)

      # HMSET Field<-->Value Pairs
      hmset = [
        "HMSET", system._systemKey(["messages", id])
        "queue", queue
        "hidden", 0 # Messages Added are By Default Unhidden
        "modified", modified
      ]


      commands = [
        hmset
        [ "ZADD", system._systemKey(["queue", queue, "M"]), modified, id ]
        [ "HINCRBY", _this.system._systemKey(["queue", queue, "P"]), "received", 1 ]
      ]

      system.server.multi(commands).exec (err, rcmds) ->
        if error? # HANDLE ERROR
          return cb error

        cb? null, id

  @__removeMessage: (system, queue, id, cb) ->
    redis = system._redis

    # Get Current Redis Server TIME
    redis.time (error, rtime) ->
      if error? # HANDLE ERROR
        return cb error

      # Modification Time : (UNIX TIMESTAMP)+SERVER MICROSENDOS
      modified = parseInt(rtime[0], 10)*1000000+parseInt(rtime[1], 10)

      # HMSET Field<-->Value Pairs
      hmset = [
        "HMSET", system._systemKey(["messages", id])
        "queue", null
        "modified", modified
      ]

      commands = [
        hmset
        [ "ZREM",  system._systemKey(["queue", queue, "M"]), id ]
        [ "ZREM",  system._systemKey(["queue", queue, "H"]), id ]
      ]

      system.server.multi(commands).exec (err, rcmds) ->
        if error? # HANDLE ERROR
          return cb error

        cb? null, id

  @__hideMessage: (system, queue, id, timeout, cb) ->
    redis = system._redis

    # REDIS: Get Current Redis Server TIME
    redis.time (error, rtime) ->
      if error? # HANDLE ERROR
        return cb error


      # Calculate Timeout : (UNIX TIMESTAMP+TIMEOUT)+SERVER MICROSENDOS
      timeout = parseInt(rtime[0] + timeout,10)*1000000+parseInt(rtime[1], 10)

      commands = [
        [ "ZREM",  system._systemKey(["queue", queue, "M"]), id ]
        [ "ZADD",  system._systemKey(["queue", queue, "H"]), timeout, id ]
        [ "HSET",  system._systemKey(["messages", id]),   "hidden", 1 ]
      ]

      redis.multi(commands).exec (error, rcmds) ->
        if error? # HANDLE ERROR
          return cb error

        cb null, true

  @__findMessage: (system, queue, id, active, cb) ->
    Message.__load system, id, (error, m) ->
      if error? # HANDLE ERROR
        return cb error

      # Do we have a Message in the Correct Queue?
      if m? and (m._queue == queue) # YES
        # Found a Valid Message
        return cb null, msg
      # ELSE: No Valid Message Found
      cb null, null

  @__pending: (system, queue, cb) ->
    redis = system._redis

    # Get Current Redis Server TIME
    redis.time (error, rtime) ->
      if error? # HANDLE ERROR
        return cb error

      # Server Time : (UNIX TIMESTAMP)+SERVER MICROSENDOS
      time = parseInt(rtime[0], 10)*1000000+parseInt(rtime[1], 10)

      # Get List of Pending Messages
      key = system._systemKey ["queue", queue, "M"]
      redis.zrangebyscore key, "-inf", time, (error, messages) ->
        if error? # HANDLE ERROR
          return cb error

        cb null, messages

  # Create a Queue Instance (Allows for Method Chaining)
  @getInstance = (system, name) ->
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
  post: (message, cb) ->
    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    @_post message, (error, m) =>
      # HANDLE ERROR
      if error?
        @emit "error", error
        return cb? error

      @emit "message-new", m
      cb? null, m

    return @

  postJSON: (json, cb) ->
    # Validate 'json' Parameter
    if _.isPlainObject json
      return @post JSON.stringify(json), cb

    throw new Error("Parameter [json] is Missing or is not an Object")

  _post: (message, cb) ->
    # Validate 'message' Parameter
    message = nullOnEmpty message
    if !message?
      throw new Error("Parameter [message] is NOT a String or is an Empty String")

    # Create and Post the Message to the Queue
    Message.__create @system, @name, message, null, cb

    return @

  # Find a Message in Current Queue
  #
  # @param [Function] (OPTIONAL) cb Callback Function, if passed will be called
  # @param [String] (REQUIRED) Message ID
  # @param [Boolean] (OPTIONAL: Default TRUE) Search for Active Messages Only?
  # @return [Object] self
  # @event error Any error generated during search
  # @event message-found Called if Message Found (message) or Not Found (null)
  find: (id, cb) ->
    # Validate Message ID
    id = validateMID id

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Message
      .__findInQueue @system, @name, id, false, (error, m) =>
        if error? # HANDLE ERROR
          @emit "error", error
          return cb? error

        @emit "message-found", m
        cb? null, m

    return @

  exists: (id, cb) ->
    # Validate Message ID
    id = validateMID id

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Message
      .__findInQueue @system, @name, id, false, (error, m) =>
        if error? # HANDLE ERROR
          @emit "error", error
          return cb? error

        exists =  if m? then true else false
        @emit "message-exists", exists
        cb? null, exists

    return @

  pending: (cb) ->
    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Queue
      .__pending @system, @name, (error, ids) =>
        if err? # HANDLE ERROR
          @emit "error", error
          return cb? error

        list = if ids? then ids else []
        @emit "messages-pending", list
        cb? null, list

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

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    # Find Pending IDs
    Queue
      .__pending @system, @name, (error, ids) =>
        if error? # HANDLE ERROR
          @emit "error", error
          return cb? error

        # Do we have a Pending Message
        if ids.length # YES: Load It
          Message.__load @system, ids[0], (error, m) =>
            if error? # HANDLE ERROR
              @emit "error", error
              return cb? error

            @emit "message-peek", m
            cb? null, m
        else # NO
          @emit "message-peek", null
          cb? null, null

    return @

  # Receive Message from Queue (if any)
  receive: (cb) ->
    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Queue
      .__pending @system, @name, (error, ids) =>
        if error? # HANDLE ERROR
          @emit "error", error
          return cb? error

        # Do we have a Pending Message
        if ids.length # YES: Load It
          Message.__load @system, ids[0], (error, m) =>
            if error? # HANDLE ERROR
              @emit "error", error
              return cb? error

            # Does the Message have a Hide Timeout
            timeout = if m._props.htimeout? then m._props.htimeout else _DEFAULTS.queue.htimeout
            Queue.__hideMessage @system, @name, m.id(), timeout, (error, ok) =>
              if error? # HANDLE ERROR
                @emit "error", error
                return cb? error

              m._props.hidden = true
              @emit "message", m
              cb? null, m

        else # NO
          @emit "message", null
          cb? null, null

    # Return SELF
    return @

  # Pop Message from Queue (Read and Delete)
  pop: (cb) ->
    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Queue
      .__pending @system, @name, (error, ids) =>
        if error? # HANDLE ERROR
          @emit "error", error
          return cb? error

        # Do we have a Pending Message
        if ids.length # YES: Load It
          Message.__load @system, ids[0], (error, m) =>
            if error? # HANDLE ERROR
              @emit "error", error
              return cb? error

            m.delete (error, m) =>
              if error? # HANDLE ERROR
                @emit "error", error
                return cb? error

              @emit "message-pop", m
              cb? null, m

        else # NO
          @emit "message-removed", null
          cb? null, null

    # Return SELF
    return @

class System extends EventEmitter
  # System Cache
  _cache: {}

  # Create a Message Instance (Allows for Method Chaining)
  @getInstance = (options, ns) ->
    new System options, ns

  # Constructor
  constructor: (options = {}, @_ns) ->
    # Do we want a separate namespace for the System?
    @_ns = (_ns + ":") if _ns?

    # Did we receive a Redis Connection?
    if options.constructor?.name is "RedisClient" # YES
      @_redis = options
    else # NO: Create New Connection
      opts = _.extend _DEFAULTS.redis, options
      @_redis = redis.createClient(opts.port, opts.host, opts.options)

    # If external client is used it might already be connected. So we check here:
    @connected = @_redis.connected or false
    if @connected
      @emit "connect", @
      @initializeRedis

    # REDIS EVENTS: Connect
    @_redis.on "connect", =>
      @connected = true
      @emit "connect", @
      @initializeRedis

    # REDIS EVENTS: Error
    @_redis.on "error", (err) =>
      if err.message.indexOf "ECONNREFUSED"
        @connected = false
        @emit "disconnect"
      else
        console.error "Redis ERROR", err
        @emit "error", err

  quit: (cb) ->
    @_redis.quit (err, resp) =>
      if err? # HANDLE ERROR
        @emit "error", err
        return cb? err, @

      @emit "disconnect", resp
      cb? null, resp

    return @

  post: (message, queue, ifexists = false, cb) ->
    # Validate 'message' Parameter
    message = nullOnEmpty message
    # Validate Queue Name
    queue = validateQNAME queue

    if !message?
      throw new Error("Parameter [message] is NOT a String or is an Empty String")

    # Handle Optional Parameters
    if _.isFunction ifexists
      cb = ifexists
      ifexists = false
    else
      ifexists = !!ifexists

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    wrapper = (error, q) =>
      # HANDLE ERROR
      if err?
        @emit "error", error
        return cb? error, @

      # Use Queue Object to Post Message
      q._post message, (error, m) =>
        # HANDLE ERROR
        if error?
          @emit "error", error
          return cb? error, @

        @emit "message-new", m, queue
        cb? null, m

    # Do Only Want to Post ONLY IF the Queue Exists?
    if ifexists # YES
      Queue
        .__load @, queue, wrapper
    else # NO: Create Queue
      Queue
        .__loadOrCreate @, queue, _DEFAULTS.queue, wrapper

    return @

  postJSON: (json, queue, ifexists = false, cb) ->
    # Validate 'json' Parameter
    if _.isPlainObject json
      return @post JSON.stringify(json), queue, ifexists, cb

    throw new Error("Parameter [json] is Missing or is not an Object")

  # List Queues in System
  queues: (cb) ->
    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Queue
      .__list @, (error, list) =>
        if error? # HANDLE ERROR
          @emit "error", error
          return cb? error

        @emit "queues", list
        cb? null, list

    return @

  queue: (name, ifexists, options, cb) ->
    # Was ifexists Set?
    if _.isFunction ifexists # NO: it is Actually the Callback
      cb = ifexists
      ifexists = false
      options = null
    else
      ifexists = !!ifexists

    # Was Options Set?
    if _.isFunction options # NO: it is Actually the Callback
      cb = options
      options = null

    # Is Options Valid or NULL?
    if options? && !_.isPlainObject options # NO: Error
      throw new Error("Parameter [options] contains an Invalid Value")

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    return @_queue name, false, options, cb

  _queue:  (name, ifexists, options, cb) ->
    # Validate Queue Name
    name = validateQNAME name

    wrapper = (error, q) =>
      if error? # HANDLE ERROR
        @emit "error", error
        return cb? error

      @emit "queue", q
      cb? null, q

    if ifexists
      Queue
        .__load @, name, wrapper
    else
      # Make Sure we Have Someting Valid for Queue Options
      if options?
        options = _.extend (
          _DEFAULTS.queue
          _.pick options, (value,key) ->
            ["htimeout", "etimeout", "plimit"].indexOf(key) > 0
          )
      else
        options = _DEFAULTS.queue

      Queue
        .__loadOrCreate @, name, options, wrapper

    return @

  existsQueue: (name, cb) ->
    # Validate Queue Name
    name = validateQNAME name

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    Queue
      .__exists @, name, (error, exists) =>
        # HANDLE ERROR
        if error?
          @emit "error", error
          return cb? error

        @emit "queue-exists", exists, name
        cb? null, exists, name

    return @

  # Find a Message Globally (in any Queue)
  #
  # @param [String] (REQUIRED) Message ID
  # @param [String] (OPTIONAL) Limit Search to Queue
  # @param [Boolean] (OPTIONAL: Default TRUE) Search for Active Messages Only?
  # @param [Function] (OPTIONAL) cb Callback Function, if passed will be called
  # @return [Object] self
  # @event error Any error generated during search
  # @event message-found Called if Message Found (message) or Not Found (null)
  find: (id, name = null, active = false, cb) ->
    # Is 'name' the callback?
    if _.isFunction name # YES: Switch Around Parameters
      cb = name
      active = false
      name = null
    # NO: Is 'active' the callback?
    else if _.isFunction active # YES: Switch Around Parameters
      cb = name
      active = false

    # Validate Message ID
    id = validateMID id

     # Valdiate Queue Name
    name = nullOnEmpty name
    if name?
      name = validateQNAME name

    # Validate 'cb'
    cb = if _.isFunction(cb) then cb else null

    # Event/Callback Wrapper
    wrapper = (error, m) =>
      if error? # HANDLE ERROR
        @emit "error", error
        return cb? error

      @emit "found", m
      cb? null, m

    if name?
      Message.__findInQueue @system, @name, id, false, wrapper
    else
      Message.__findInAll @system, id, false, wrapper

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
