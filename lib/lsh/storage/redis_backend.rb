# ruby-lsh
#
# Copyright (c) 2012 British Broadcasting Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'redis'
require 'json'

module LSH

  module Storage

    class RedisBackend

      attr_reader :redis

      def initialize(params = { :redis => { :host => '127.0.0.1', :port => 6379 }})
        @redis = Redis.new(params[:redis])
      end

      def reset!
        clear_data!
        clear_projections!
      end

      def clear_data!
        keys = @redis.keys("lsh:bucket:*")
        @redis.del(keys) unless keys.empty?
        keys = @redis.keys("lsh:vector_to_id:*")
        @redis.del(keys) unless keys.empty?
        keys = @redis.keys("lsh:id_to_vector:*")
        @redis.del(keys) unless keys.empty?
        keys = @redis.keys("lsh:vector:*")
        @redis.del(keys) unless keys.empty?
      end

      def clear_projections!
        @redis.del("lsh:parameters")
        @redis.del("lsh:buckets")
        keys = @redis.keys("lsh:projection:*")
        @redis.del(keys) unless keys.empty?
      end

      def has_index?
        parameters and projections and number_of_buckets > 0
      end

      def number_of_buckets
        @redis.get("lsh:buckets").to_i || 0
      end

      def projections=(projections)
        projections.each_with_index do |projection, i|
          @redis.set("lsh:projection:#{i}", projection.to_binary)
        end
      end

      def projections
        return unless parameters
        @projections ||= (
          (0...parameters[:number_of_independent_projections]).map do |i|
            m = MathUtil.zeros(parameters[:number_of_random_vectors],
                               parameters[:dim])
            m.from_binary(@redis.get("lsh:projection:#{i}"))
          end
        )
      end

      def parameters=(parms)
        parms[:window] = 'Infinity' if parms[:window] == Float::INFINITY
        @redis.set "lsh:parameters", parms.to_json
      end

      def parameters
        begin
          @parms ||= (
            parms = JSON.parse(@redis.get "lsh:parameters")
            parms.keys.each { |k| parms[k.to_sym] = parms[k]; parms.delete(k) }
            parms[:window] = Float::INFINITY if parms[:window] == 'Infinity'
            parms
          )
        rescue TypeError
          nil
        end 
      end

      def create_new_bucket
        @redis.incr "lsh:buckets"
      end

      def save_vector(vector, vector_hash)
        @redis.set "lsh:vector:#{vector_hash}", vector.to_binary
      end

      def load_vector(hash)
        MathUtil.zeros(1, parameters[:dim])
                .from_binary(@redis.get "lsh:vector:#{hash}")
      end

      def add_vector(vector, vector_hash)
        save_vector(vector, vector_hash) # Writing vector to disk if not already there
      end

      def add_vector_hash_to_bucket(bucket, hash, vector_hash)
        @redis.sadd "#{bucket}:#{hash}", vector_hash.to_s # Only storing vector's hash in Redis
      end

      def add_vector_id(vector_hash, id)
        @redis.set "lsh:vector_to_id:#{vector_hash}", id
        @redis.set "lsh:id_to_vector:#{id}", vector_hash.to_s
      end

      def vector_hash_to_id(vector_hash)
        @redis.get "lsh:vector_to_id:#{vector_hash}"
      end

      def id_to_vector(id)
        vector_hash = @redis.get "lsh:id_to_vector:#{id}"
        load_vector(vector_hash)
      end

      def find_bucket(i)
        "lsh:bucket:#{i}"
      end

      def query_buckets(hashes)
        keys = hashes.each_with_index.map do |hash, i|
          bucket = find_bucket(i)
          "#{bucket}:#{hash}"
        end
        results_hashes = @redis.sunion(keys)

        results_hashes.map do |vector_hash|
          {
            :data => load_vector(vector_hash),
            :hash => vector_hash.to_i,
            :id => vector_hash_to_id(vector_hash)
          }
        end
      end

    end

  end

end
