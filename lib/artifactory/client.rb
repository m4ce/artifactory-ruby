#
# client.rb
#

require 'net/http'
require 'uri'
require 'openssl'
require 'json'
require 'time'

module Artifactory
  class Client
    attr_reader :uri, :http, :basic_auth, :headers

    # Initialize an Artifactory client instance
    #
    # @param endpoint [String] Artifactory REST API endpoint
    # @param username [String] Username for HTTP basic authentication
    # @param password [String] Password for HTTP basic authentication
    # @param api_key [String] API key
    # @param ssl_verify [Boolean] Enable/Disable SSL verification
    #
    def initialize(endpoint:, username: nil, password: nil, api_key: nil, ssl_verify: true)
      basic_auth = {}
      uri = URI.parse(endpoint)
      http = Net::HTTP.new(uri.host, uri.port)

      if (username and api_key) or (username.nil? and api_key.nil?)
        raise RuntimeError, "Either HTTP basic or API key are allowed as authentication methods"
      end

      headers = {
        'Content-type' => 'application/json',
        'Accept' => 'application/json',
      }

      if username
        basic_auth = {'username' => username, 'password' => password}
      else
        headers['X-JFrog-Art-Api'] = api_key
      end

      if uri.scheme == 'https'
        http.use_ssl = true
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE unless ssl_verify
      end

      @uri = uri
      @http = http
      @basic_auth = basic_auth
      @headers = headers
    end

    # Retrieves the current configuration of a repository. Supported by local, remote and virtual repositories.
    #
    # @param key [String] Repository key
    # @return [Hash] Repository information
    #
    def get_repo(key:)
      api_get("/repositories/#{key}").tap { |h| h.delete('key') }
    end

    # Returns a list of minimal repository details (unless recurse is enabled) for all repositories of the specified type.
    #
    # @param type [nil, local, remote, virtual] Optionally filter by repository type
    # @param recurse [Boolean] Recursively retrieve repos configuration
    # @return [Hash] List of repositories
    #
    def repos(type: nil, recurse: false)
      ret = {}
      params = []
      params << "type=#{type}" if type

      api_get(["/repositories", params.join('&')].join('?')).each do |repo|
        ret[repo['key']] = recurse ? self.get_repo(key: repo['key']) : repo.tap { |h| h.delete('key') }
      end

      ret
    end

    # Lists all Docker repositories hosted in under an Artifactory Docker repository
    #
    # @param repo_key [String] Repository key
    # @param recurse [Boolean] Recursively retrieve image tags
    # @return [Hash, Array<String>] List of docker images
    #
    def docker_images(repo_key:, recurse: false)
      ret = {}
      repolist = api_get("/docker/#{repo_key}/v2/_catalog")['repositories']

      if recurse
        api_get("/docker/#{repo_key}/v2/_catalog")['repositories'].each do |name|
          ret[name] = self.docker_tags(repo_key: repo_key, image_name: name)
        end
      else
        ret = repolist
      end

      images
    end

    # Retrieve all tags for a docker image
    #
    # @param repo_key [String] Repository key
    # @param image_name [String] Docker image name
    # @return [Array<String>] List of tags
    #
    def docker_tags(repo_key:, image_name:)
      api_get("/docker/#{repo_key}/v2/#{image_name}/tags/list")['tags']
    end

    # Retrieve a docker image tag manifest
    #
    # @param repo_key [String] Repository key
    # @param image_name [String] Docker image name
    # @param image_tag [String] Docker image tag
    # @return [Hash] Docker manifest describing the tag
    #
    def docker_manifest(repo_key:, image_name:, image_tag:)
      api_get("/docker/#{repo_key}/v2/#{image_name}/manifests/#{image_tag}")
    end

    # Get a flat (the default) or deep listing of the files and folders (not included by default) within a folder
    #
    # @param repo_key [String] Repository key
    # @param image_name [String] Docker image name
    # @param image_tag [String] Docker image tag
    # @return [Hash] Docker manifest describing the tag
    #
    def file_list(repo_key:, folder_path: '/', deep: false, depth: 0, list_folders: false, md_timestamps: false, include_root_path: false)
      path = ["/storage", repo_key, folder_path].join('/').chomp('/')
      params = ['list']
      params << "deep=#{deep ? 1 : 0}"
      params << "depth=#{depth}" if depth > 0
      params << "listFolders=#{list_folders ? 1 : 0}"
      params << "mdTimestamps=#{md_timestamps ? 1 : 0}"
      params << "includeRootPath=#{include_root_path ? 1 : 0}"

      files = {}
      api_get([path, params.join('&')].join('?'))['files'].each do |file|
        name = file['uri']
        files[name] = file.tap { |h| h.delete('uri') }
        files[name]['lastModified'] = Time.parse(files[name]['lastModified'])
      end

      files
    end

    # Get file information like last modification time, creation time etc.
    #
    # @param repo_key [String] Repository key
    # @param path [String] Path of the file to look up
    # @return [Hash] File information
    #
    def file_info(repo_key:, path:)
      ret = {}

      api_get(File.join("/storage", repo_key, path).chomp('/')).each do |k, v|
        case k
          when "created", "lastModified", "lastUpdated"
            ret[k] = Time.parse(v)

          else
            ret[k] = v
        end
      end

      ret
    end

    # Get file statistics like the number of times an item was downloaded, last download date and last downloader.
    #
    # @param repo_key [String] Repository key
    # @param path [String] Path of the file to look up
    # @return [Hash] File statistics
    #
    def file_stat(repo_key:, path:)
      ret = {}

      p = File.join("/storage", repo_key, path).chomp('/')
      params = ['stats']

      api_get([p, params.join('&')].join('?')).tap { |h| h.delete('uri') }.each do |k, v|
        case k
          when "lastDownloaded", "remoteLastDownloaded"
            ret[k] = Time.at(v/1000) if v > 0

          else
            ret[k] = v
        end
      end

      ret
    end

    # Deletes a file or a folder from the specified destination
    #
    # @param repo_key [String] Repository key
    # @param path [String] Path of the file to delete
    #
    def file_delete(repo_key:, path:)
      api_delete(File.join(repo_key, path))
    end

    # Retrieve all artifacts not downloaded since the specified Java epoch in milliseconds
    #
    # @param repo_key [String, Array<String>] Repository key(s)
    # @param not_used_since [Time] Return artifacts that have not been used since the given date
    # @param created_before [Time] Return artifacts that have been created before the given date
    # @return [Hash] Artifacts matching search criteria
    #
    def search_usage(repo_key:, not_used_since:, created_before: nil)
      ret = {}

      path = ["/search", "usage"]
      params = []
      params << "notUsedSince=#{not_used_since.to_f.round(3) * 1000.to_i}"
      params << "createdBefore=#{created_before.to_f.round(3) * 1000.to_i}" if created_before
      params << "repos=#{repo_key.is_a?(Array) ? repo_key.join(',') : repo_key}"

      api_get([path, params.join('&')].join('?'))['results'].each do |result|
        result.each do |k, v|
          case
            when "lastDownloaded", "remoteLastDownloaded"
              ret[result['uri']] = Time.parse(v)

            when "uri"
              next

            else
              ret[result['uri']] = v
          end
        end
      end

      ret
    end

    # Get all artifacts with specified dates within the given range
    #
    # @param repo_key [String, Array<String>] Repository key(s)
    # @param from_date [Time] Return artifacts that have not been used since the given date
    # @param to_date [Time] Return artifacts that have been created before the given date
    # @param fields [created, lastModified, lastDownloaded] Date fields that specify which fields the from_date and to_date values should be applied to
    # @return [Hash] Artifacts matching search criteria
    #
    def search_dates(repo_key:, from_date:, to_date: Time.now, date_fields:)
      ret = {}

      valid_date_fields = ["created", "lastModified", "lastDownloaded"]

      date_fields.each do |date_field|
        raise ValueError, "Not a valid date field '#{date_field}'" unless valid_date_fields.include?(date_field)
      end

      path = ["/search", "dates"]
      params = []
      params << "from=#{from_date.to_f.round(3) * 1000.to_i}" unless from_date.nil?
      params << "to=#{to_date.to_f.round(3) * 1000.to_i}" unless to_date.nil?
      params << "repos=#{repo_key.is_a?(Array) ? repo_key.join(',') : repo_key}"
      params << "dateFields=#{date_fields.join(',')}"

      api_get([path, params.join('&')].join('?'))['results'].each do |result|
        result.each do |k, v|
          case k
            when *valid_date_fields
              ret[result['uri']] = Time.parse(v)

            when "uri"
              next

            else
              ret[result['uri']] = v
          end
        end
      end

      ret
    end

    # Get all artifacts created in date range
    #
    # @param repo_key [String, Array<String>] Repository key(s)
    # @param from_date [Time] Return artifacts that have not been used since the given date
    # @param to_date [Time] Return artifacts that have been created before the given date
    # @return [Hash] Artifacts matching search criteria
    #
    def search_creation(repo_key:, from_date:, to_date: Time.now)
      ret = {}

      path = ["/search", "creation"]
      params = []
      params << "from=#{from_date.to_f.round(3) * 1000.to_i}" unless from_date.nil?
      params << "to=#{to_date.to_f.round(3) * 1000.to_i}" unless to_date.nil?
      params << "repos=#{repo_key.is_a?(Array) ? repo_key.join(',') : repo_key}"

      api_get([path, params.join('&')].join('?'))['results'].each do |result|
        result.each do |k, v|
          case
            when "created"
              ret[result['uri']] = Time.parse(v)

            when "uri"
              next

            else
              ret[result['uri']] = v
          end
        end
      end

      ret
    end

    # Get all artifacts matching the given path pattern
    #
    # @param repo_key [String] Repository key
    # @param pattern [String] File pattern
    # @return [Hash] Artifacts matching search pattern
    #
    def search_pattern(repo_key:, pattern:)
      path = ["/search", "pattern"]
      params = ["pattern=#{repo_key}:#{pattern}"]

      api_get([path, params].join('?'))['results']
    end

private
    # Dispatch a GET request to the Artifactory API interface
    #
    # @param query [String] HTTP request query
    # @return Response from the server
    #
    def api_get(query)
      begin
        req = Net::HTTP::Get.new(File.join(self.uri.path, 'api', query), self.headers)
        req.basic_auth(self.basic_auth['username'], self.basic_auth['password']) if self.basic_auth
        resp = self.http.request(req)

        if resp.is_a?(Net::HTTPOK)
          begin
            data = JSON.parse(resp.body)
          rescue JSON::ParserError
            raise Exception, "Failed to decode response message"
          end
        else
          raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})"
        end
      rescue
        raise Exception, "Failed to execute GET request to Artifactory REST API (#{$!})"
      end

      data
    end

    # Dispatch a DELETE request to the Artifactory API interface
    #
    # @param query [String] HTTP request query
    #
    def api_delete(query)
      begin
        req = Net::HTTP::Delete.new(File.join(self.uri.path, 'api', query), self.headers)
        req.basic_auth(self.basic_auth['username'], self.basic_auth['password']) if self.basic_auth
        resp = self.http.request(req)

        raise Exception, "Query returned a non successful HTTP code (Code: #{resp.code}, Error: #{resp.message})" unless resp.is_a?(Net::HTTPNoContent)
      rescue
        raise Exception, "Failed to execute DELETE request to Artifactory REST API (#{$!})"
      end
    end
  end
end
