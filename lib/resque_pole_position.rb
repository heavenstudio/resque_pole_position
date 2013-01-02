require "resque"

module Resque
  class Job
    def self.create_at_pole_position(queue, klass, *args)
      Resque.validate(klass, queue)

      if Resque.inline?
        # Instantiating a Resque::Job and calling perform on it so callbacks run
        # decode(encode(args)) to ensure that args are normalized in the same manner as a non-inline job
        new(:inline, {'class' => klass, 'args' => decode(encode(args))}).perform
      else
        Resque.shift(queue, :class => klass.to_s, :args => args)
      end
    end
  end

  def shift(queue, item)
    watch_queue(queue)
    redis.lpush "queue:#{queue}", encode(item)
  end

  def pole_position(klass, *args)
    queue = queue_from_class(klass)

    # Perform before_enqueue hooks. Don't perform enqueue if any hook returns false
    before_hooks = Plugin.before_enqueue_hooks(klass).collect do |hook|
      klass.send(hook, *args)
    end
    return nil if before_hooks.any? { |result| result == false }

    Job.create_at_pole_position(queue, klass, *args)

    Plugin.after_enqueue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end

    return true
  end
end