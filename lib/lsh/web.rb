require 'sinatra'
require 'json'
require 'time'

module LSH

  class Web < Sinatra::Base

    attr_reader :index

    def initialize(index)
      super
      @index = index
    end

    get '/' do
      content_type :json
      { :index => index.inspect }.to_json
    end

    post '/query' do
      raise "Missing query" unless params[:data]
      include_data = {nil => true, "true" => true, "false" => false}[params[:include_data]]
      raise "include_data must be 'true' or 'false'." if include_data.nil?
      mime_type = (params[:mime_type] || 'application/json')
      if mime_type == 'application/json'
        t0 = Time.now
        vector = JSON.parse(params[:data], :create_additions => true)
        min_similarity = params[:min_similarity].to_f if params[:min_similarity]
        results = index.query(vector, params[:radius] || 0, min_similarity)
        if not include_data
          results = results.map { |result| result.merge(:data => nil) }
        end
        content_type :json
        { "time" => Time.now - t0, "results" => results }.to_json
      else
        raise "Unrecognised mime-type"
      end
    end

    post '/query-ids' do
      if params[:data] # We're querying with a vector
        mime_type = (params[:mime_type] || 'application/json')
        if mime_type == 'application/json'
          t0 = Time.now
          vector = JSON.parse(params[:data], :create_additions => true)
          results = index.query_ids_by_vector(vector, params[:radius] || 0)
          content_type :json
          { "time" => Time.now - t0, "results" => results }.to_json
        else
          raise "Unrecognised mime-type"
        end
      elsif params[:id] # We're querying with an id
        raise "Unknown id" unless index.id_to_vector(params[:id])
        t0 = Time.now
        results = index.query_ids(params[:id], params[:radius] || 0)
        content_type :json
        { "time" => Time.now - t0, "results" => results }.to_json
      else
        raise "Missing query"
      end
    end

    post '/index' do
      raise "Missing data" unless params[:data]
      mime_type = (params[:mime_type] || 'application/json')
      if mime_type == 'application/json'
        t0 = Time.now
        vector = JSON.parse(params[:data], :create_additions => true)
        index.add(vector, params[:id])
        content_type :json
        { "time" => Time.now - t0, "status" => "indexed" }.to_json
      else
        raise "Unrecognised mime-type"
      end
    end

  end

end
