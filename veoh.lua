local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs({
    ["^https?://veoh%.com/watch/([0-9a-zA-Z]+)$"]="video",
    ["^https?://veoh%.com/users/([^?&;/]+)$"]="user",
    ["^https?://veoh%.com/list/([^/]+/[0-9a-zA-Z_]+)$"]="list",
  }) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      context = {["pagination"]={}}
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      is_new_design = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  local skip = false
  for pattern, type_ in pairs({
    ["^https?://[^/]*veoh%.com/users/([^%?&;/]+)$"]="user",
    ["^https?://[^/]*veoh%.com/watch/getVideo/([0-9a-zA-Z]+)"]="video",
    ["^https?://[^/]*veoh%.com/watch/([0-9a-zA-Z]+)$"]="video",
    ["^https?://[^/]*veoh%.com/list/([^/]+/[0-9a-zA-Z_]+)$"]="list",
    
  }) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*veoh%.com/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  for _, pattern in pairs({
    "([a-z0-9A-Z]+)",
    "([^%?&;]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  local body_data = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      if body_data ~= nil then
--print('POSTing', url)
        table.insert(urls, {
          url=url,
          method="POST",
          body_data=body_data,
          headers={
            ["Content-Type"]="application/json",
            ["X-CSRF-TOKEN"]=context["csrf"],
            ["X-Requested-With"]="XMLHttpRequest"
          }
        })
      else
        table.insert(urls, {
          url=url_
        })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  local function extract_json(json)
    for k, v in pairs(json) do
      if type(v) == "table" then
        extract_json(v)
      else
        if k == "nickname" or k == "username" then
          check("https://veoh.com/users/" .. v)
        elseif k == "permalinkId" then
          check("https://veoh.com/watch/" .. v)
        --[[elseif k == "category" then
          local category = v
          if string.match(category, "^category_") then
            category = string.match(category, "^[^_]+_(.+)$")
          end
          check("https://veoh.com/list/videos/" .. category)]]
        end
      end
    end
  end

  local function queue_with_body(url, body)
    body_data = body
    check(url)
    body_data = nil
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://[^/]*veoh%.com/file/f/.") then
    html = read_file(file)
    if string.match(html, "^%s+{.+}") then
      json = cjson.decode(html)
    end
    if string.match(url, "^https?://veoh%.com/watch/[^/%?&;]+$") then
      local csrf = string.match(html, 'csrfToken:%s*"([^"]+)"')
      if not csrf then
        error("No CSRF token found.")
      end
      context["csrf"] = csrf
      check("https://veoh.com/watch/getVideo/" .. item_value)
      --local categories = string.match(html, "availableCategories:%s*(%[[^\n]+)")
      --context["categories"] = cjson.decode(categories)
    elseif string.match(url, "^https?://[^/]*/watch/getVideo/")
      and not context["404"] then
      ids[json["video"]["src"]["poster"]] = true
      --json["video"]["src"]["Regular"] = nil
      --html = cjson.encode(json)
      if json["video"]["allowComments"] then
        queue_with_body("https://veoh.com/watch/" .. item_value .. "/comments/1", "")
      end
    elseif string.match(url, "^https?://[^/]*/users/[^%?&/]+)") then
      queue_with_body(
        "https://veoh.com/users/find-by-username",
        cjson.encode({
          ["username"]=item_value
        })
      )
    elseif string.match(url, "^https?://[^/]+/users/find-by-username") then
      for endpoint, data in pairs({
        ["/published/videos"]={
          ["details"]={
            ["requestName"]="userVideos",
            ["getFromMyprofile"]=true
          },
        },
        ["/favorites"]={
          ["main"]={["getFromMyprofile"]=false},
          ["details"]={["requestName"]="userFavoritesVideos"},
        },
        ["/published/groups"]={
          ["details"]="x"
        },
        ["/groups/published"]={
          ["main"]="x",
          ["details"]={["requestName"]="userPublishedGroup"},
        },
        ["/groups/joined"]={
          ["details"]={["requestName"]="userJoinedGroup"},
        },
      }) do
        for _, type_name in pairs({"main", "details"}) do
          if not data[type_name] then
            data[type_name] = {}
          end
          if data[type_name] ~= "x" then
            local body = {
              ["username"]=item_value,
              ["page"]=1,
            }
            for k, v in pairs(({
              ["details"]={
                ["maxResults"]="16",
                ["getFromMyprofile"]=false
              },
              ["main"]={
                ["maxResults"]=4,
                ["requestName"]="userPage"
              }
            })[type_name]) do
              body[k] = v
            end
            for k, v in pairs(data[type_name]) do
              body[k] = v
            end
            local newurl = "https://veoh.com/users" .. endpoint
            if type_name == "details" then
              context["pagination"][newurl] = body
            end
            queue_with_body(newurl, cjson.encode(body))
          end
        end
      end
      queue_with_body("https://veoh.com/users/get/" .. item_value, "")
      for _, page in pairs({
        "published-videos",
        "favorites-videos",
        "published-groups",
        "joined-groups"
      }) do
        check("https://veoh.com/users/" .. item_value .. "/" .. page)
      end
    elseif string.match(url, "/comments/[0-9]+$") then
      local pages = tonumber(json["totalRecord"]) / tonumber(json["recordPerPage"])
      for i=1,pages do
        queue_with_body(urlparse.absolute(url, tostring(i)), "")
      end
    elseif string.match(url, "^https?://[^/]*/list%-c/") then
      queue_with_body(
        "https://veoh.com/collectionByPermalink/" .. item_value,
        cjson.encode({["permalink"]=item_value})
      )
    elseif string.match(url, "/collectionByPermalink/") then
      for _, key in pairs({
        "medResImageUrl",
        "highResImageUrl"
      }) do
        local image = json[key]
        if image then
          ids[image] = true
        end
      end
      local body = {
        ["collectionId"]=json["collection"]["collectionId"],
        ["page"]=1
      }
      for _, endpoint in pairs({
        "/group-info/comments",
        "/list/group/videos"
      }) do
        local newurl = "https://veoh.com" .. endpoint
        context["pagination"][newurl] = body
        queue_with_body(newurl, cjson.encode(body))
      end
    elseif string.match(url, "/list/groups/groups_") -- TODO
      or string.match(url, "/list/videos/")
      or string.match(url, "/list/movies/")
      or string.match(url, "/list/music/music_")
      or string.match(url, "/list/webseries/webseries_") then
      local newurl = "https://veoh.com/list/category/collections"
      local bodies = {}
      for _, sorting in pairs({
        "most recent",
        "most viewed",
        "title"
      }) do
        local body = {
            ["category"]="groups",
            ["filters"]={
              ["sort"]=sorting
            },
            ["page"]=1,
            ["pages"]=10,
            ["subCategory"]="groups_" .. item_value
          }
        bodies[sorting] = body
        queue_with_body(newurl, cjson.encode(body))
      end
      context["pagination"][newurl] = {}
    end
    if string.match(url, "/group%-info/comments$")
      or string.match(url, "/list/group/videos$")
      or string.match(url, "/list/category/collections$") then
      local per_page = json["recordPerPage"]
      if not per_page then
        per_page = 18
      end
      local pages = tonumber(json["totalRecords"]) / per_page
      for i=1,pages do
        local bodies = context["pagination"][url]
        if not string.match(url, "/list/category/collections$") then
          bodies = {bodies}
        end
        for _, body in pairs(bodies) do
          body["page"] = i
          check(url, cjson.encode(body))
        end
      end
    end
    if string.match(url, "/users/published/videos$")
      or string.match(url, "/users/favorites$")
      or string.match(url, "/users/published/groups$")
      or string.match(url, "/users/groups/published$")
      or string.match(url, "/users/groups/joined$") then
      local count = 0
      local key = nil
      for _, k in pairs({"videos", "groups"}) do
        if json[k] then
          key = k
        end
      end
      for _ in pairs(json[key]) do
        count = count + 1
      end
      local body = context["pagination"][url]
      if count == body["maxResults"] then
        local pages = json["totalRecords"] / body["maxResults"]
        for i=1,pages do
          body["page"] = i
          queue_with_body(url, cjson.encode(body))
        end
      end
    end
    if json then
      extract_json(json)
      html = html .. " " .. flatten_json(json)
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  is_new_design = false
  is_good_404 = false
  if http_stat["statcode"] == 404
    and not string.match(url["url"], "^https?://veoh%.com/.")
    and string.match(url["url"], "^https?://[^/]+%.veoh%.com/.") then
    is_good_404 = true
  end
  if http_stat["statcode"] == 302 then
    if not string.match(url["url"], "^https?://redirect%.veoh%.com/.") then
      retry_url = true
      return false
    end
  elseif http_stat["statcode"] ~= 200
    and not is_good_404 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0 and not is_good_404 then
    retry_url = true
    return false
  end
  if string.match(url["url"], "^https?://[^/]*veoh%.com/.") then
    local html = read_file(http_stat["local_file"])
    if not is_good_404
      and http_stat["statcode"] ~= 302
      and string.len(string.match(html, "%s*(.)")) == 0 then
      retry_url = true
      return false
    end
    if string.match(html, "^%s*{") then
      local json = cjson.decode(html)
      if not json["success"] or json["error"] then
        if json["error"] == "404-error" then
          context["404"] = true
        else
          retry_url = true
          return false
        end
      end
    end
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end
  
  if is_new_design then
    return wget.actions.EXIT
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(url["url"], "^https?://redirect%.veoh%.com/.") then
      ids[newloc] = true
    end
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 5
    if context["404"]
      or status_code == 404 then
      maxtries = 0
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["veoh-dr7zjzk9jwjnzhe0"] = discovered_items,
    ["urls-xab7i5hl05ucp584"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


