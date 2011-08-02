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
#
#
class ResqueScheduler extends EventEmitter
  constructor: (resque) ->
    @resque  = resque
    @redis   = @resque.redis
    @running = false
    
  enqueueAt: (queue, future, command, args) ->
    job = JSON.stringify class: command, queue: queue, args: args || []
    @delayDelivery future, job

  enqueueIn: (queue, secondsFromNow, command, args) ->
    future = @now() + secondsFromNow * 1000
    @enqueueAt queue, future, command, args

  delayDelivery: (future, job) ->
    future = Helpers.timestamp future
    multi  = @redis.multi()
    multi.rpush @resque.key("delayed:#{future}"), job
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
        @deliverJobs timestamp, => @poll()
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
    key = @resque.key('delayed_queue_schedule')
    @redis.zrangebyscore key, '-inf', time, 'limit', 0, 1, (err, items) ->
      callback items[0]
        
  #
  # deliver the delayed jobs to their targeted queues
  #
  deliverJobs: (timestamp, callback) ->
    @jobsForDelivery timestamp, (jobs) =>
      @deliver job for job in jobs
      callback()
        
  #
  # fetch all the jobs at the delayed timeslot
  #
  jobsForDelivery: (timestamp, callback) ->
    key = @resque.key "delayed:#{timestamp}"
    @redis.lrange key, 0, -1, (err, jobs) =>
      @cleanup timestamp, =>
        result = []
        result.push JSON.parse job for job in jobs
        callback result

  #
  # enqueue the delayed job with resque
  #
  deliver: (job) ->
    log "Delivering job #{job.class}"
    @resque.enqueue job.queue, job.class, job.args
  
  #
  # delete the timestamp from the delay queue
  #
  cleanup: (timestamp, callback) ->
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
    
