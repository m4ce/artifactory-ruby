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

    def get_repo(key:)
      api_get("/repositories/#{key}").tap { |h| h.delete('key') }
    end

    def repos(type: nil, recurse: false)
      ret = {}
      params = []
      params << "type=#{type}" if type

      api_get(["/repositories", params.join('&')].join('?')).each do |repo|
        ret[repo['key']] = recurse ? self.get_repo(key: repo['key']) : repo.tap { |h| h.delete('key') }
      end

      ret
    end

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

    def docker_tags(repo_key:, image_name:)
      api_get("/docker/#{repo_key}/v2/#{image_name}/tags/list")['tags']
    end

    def docker_manifest(repo_key:, image_name:, image_tag:)
      api_get("/docker/#{repo_key}/v2/#{image_name}/manifests/#{image_tag}")
    end

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

    def file_stat(repo_key:, path:)
      p = File.join("/storage", repo_key, path).chomp('/')
      params = ['stats']

      ret = {}
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

    def file_delete(repo_key:, path:)
      api_delete(File.join(repo_key, path))
    end

private
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
