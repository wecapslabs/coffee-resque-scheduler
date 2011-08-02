{log}        = require 'util'
EventEmitter = require('events').EventEmitter

Helpers =
  timestamp: (val) ->
    Math.floor @getTime val
    
  getTime: (val) ->
    if @isDate val
      val.getTime() / 1000
    else if @isNumber val
      val / 1000
    else
      throw "Invalid timestamp provide. Should be either a Date object or a number."    
    
  isDate:   (val) -> '[object Date]' is toString.apply val
  isNumber: (val) -> typeof val is 'number' and isFinite val
 
#        
# Maintains the actual queue that will be processing the scheduled jobs.
# Most of the logic is ported over from the Ruby Resque Scheduler,
# so I tried to keep the names and rough functionality the same so
# the can be compatible and such.
#
class ResqueScheduler extends EventEmitter
  constructor: (resque) ->
    @resque  = resque
    @redis   = @resque.redis
    @running = false
    
  enqueueAt: (queue, timestamp, command, args) ->
    item = JSON.stringify class: command, queue: queue, args: args || []
    @delayedPush timestamp, item

  enqueueIn: (queue, numberOfSecondsFromNow, command, args) ->
    newTime = @now() + numberOfSecondsFromNow * 1000
    @enqueueAt queue, newTime, command, args

  delayedPush: (delay, item) ->
    future = Helpers.timestamp delay
    multi  = @redis.multi()
    multi.rpush @resque.key("delayed:#{future}"), item
    multi.zadd  @resque.key('delayed_queue_schedule'), future, future
    multi.exec()
    
  start: ->
    return if @running
    @running = true
    @poll()
  
  end: ->
    @running = false

  #
  # poll for all timestamps between between now and in the past
  #    
  poll: ->
    time = Helpers.timestamp @now()
    @nextDelayedTimestamp time, (timestamp) =>
      if timestamp
        @enqueueDelayedItemsForTimestamp timestamp, => @poll()
      else
        @pause()
  
  #
  # ran out of delayed jobs to transfer
  #
  pause: ->
    setTimeout =>
      return unless @running
      @poll()
    , 1000
    
  #
  # fetch the next timestamp in the delay queue
  #
  nextDelayedTimestamp: (time, callback) ->
    key  = @resque.key('delayed_queue_schedule')
    @redis.zrangebyscore key, '-inf', time, 'limit', 0, 1, (err, items) ->
      callback items[0]
        
  #
  # fetch the original jobs and transfer them to their target queues
  #
  enqueueDelayedItemsForTimestamp: (timestamp, callback) ->
    @itemsForTimestamp timestamp, (jobs) =>
      @transfer job for job in jobs
      callback()
        
  #
  # fetch all the jobs at the delayed timeslot
  #
  itemsForTimestamp: (timestamp, callback) ->
    key = @resque.key "delayed:#{timestamp}"
    @redis.lrange key, 0, -1, (err, jobs) =>
      @cleanupTimestamp timestamp, =>
        result = []
        result.push JSON.parse job for job in jobs
        callback result

  #
  # enqueue the delayed job with resque
  #
  transfer: (job) ->
    log "Transfering job. #{job.class}"
    @resque.enqueue job.queue, job.class, job.args
  
  #
  # delete the timestamp from the delay queue
  #
  cleanupTimestamp: (timestamp, callback) ->
    key = @resque.key("delayed:#{timestamp}")
    multi = @redis.multi()
    multi.del  key
    multi.zrem @resque.key('delayed_queue_schedule'), timestamp
    multi.exec -> callback()
    
  #
  # current time in millis
  #
  now: -> new Date().getTime()
        
exports.schedulerUsing = (Resque) ->
  new exports.ResqueScheduler Resque || {}

exports.ResqueScheduler = ResqueScheduler
    
