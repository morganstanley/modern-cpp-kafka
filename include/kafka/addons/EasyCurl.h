#pragma once

#include <kafka/Project.h>

#include <curl/curl.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>


namespace KAFKA_API {

class EasyCurlPostRequest
{
public:
    EasyCurlPostRequest(const std::string& url)
        : _handle(curl_easy_init()),
          _headerList(new (curl_slist*)(nullptr))
    {
        setUrl(url);
    }

    EasyCurlPostRequest& setUrl(const std::string& url)
    {
        _url = url;

        curl_easy_setopt(_handle.get(), CURLOPT_URL, _url.c_str());

        return *this;
    }

    EasyCurlPostRequest& setHeaders(const std::vector<std::string>& headers)
    {
        _headers = headers;

        for (const std::string& header: _headers)
        {
            *_headerList = curl_slist_append(*_headerList, header.c_str());
        }

        curl_easy_setopt(_handle.get(), CURLOPT_HTTPHEADER, *_headerList);

        return *this;
    }

    EasyCurlPostRequest& setPayload(const std::string& payload)
    {
        _payload = payload;

        curl_easy_setopt(_handle.get(), CURLOPT_POSTFIELDSIZE, _payload.size());
        curl_easy_setopt(_handle.get(), CURLOPT_POSTFIELDS, _payload.c_str());

        return *this;
    }

    EasyCurlPostRequest& setVerbose(bool flag = true)
    {
        curl_easy_setopt(_handle.get(), CURLOPT_VERBOSE, flag ? 1L : 0L);

        return *this;
    }

    EasyCurlPostRequest& setCookie(const std::string& cookie)
    {
        _cookie = cookie;

        curl_easy_setopt(_handle.get(), CURLOPT_COOKIE, _cookie.c_str());

        return *this;
    }

    EasyCurlPostRequest& setConnectionTimeout(std::chrono::milliseconds timeout)
    {
        curl_easy_setopt(_handle.get(), CURLOPT_CONNECTTIMEOUT_MS, timeout.count());

        return *this;
    }

    EasyCurlPostRequest& setTimeout(std::chrono::milliseconds timeout)
    {
        curl_easy_setopt(_handle.get(), CURLOPT_TIMEOUT_MS, timeout.count());

        return *this;
    }

    EasyCurlPostRequest& setFollowLocation(bool flag = true)
    {
         curl_easy_setopt(_handle.get(), CURLOPT_FOLLOWLOCATION, flag ? 1L : 0L);

         return *this;
    }

    EasyCurlPostRequest& setNoprogress(bool flag = true)
    {
        curl_easy_setopt(_handle.get(), CURLOPT_NOPROGRESS, flag ? 1L : 0L);

        return *this;
    }

    CURLcode send()
    {
        _respBody.clear();
        curl_easy_setopt(_handle.get(), CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(_handle.get(), CURLOPT_WRITEDATA, &_respBody);

        _respHeaders.clear();
        curl_easy_setopt(_handle.get(), CURLOPT_HEADERFUNCTION, headerCallback);
        curl_easy_setopt(_handle.get(), CURLOPT_HEADERDATA, &_respHeaders);

        // https://curl.se/libcurl/c/curl_easy_strerror.html
        return curl_easy_perform(_handle.get());
    }

    std::string responseBody() const
    {
        return _respBody;
    }

    std::vector<std::string> responseHeaders() const
    {
        return _respHeaders;
    }

    std::string contentType()
    {
        char *contentType = nullptr;
        curl_easy_getinfo(_handle.get(), CURLINFO_CONTENT_TYPE, &contentType);
        return contentType;
    }


private:
    struct HandleCleaner { void operator()(CURL* p) { curl_easy_cleanup(p); } };
    struct HeadersCleaner { void operator()(curl_slist** pp) { curl_slist_free_all(*pp); } };

    std::unique_ptr<CURL, HandleCleaner>         _handle;
    std::unique_ptr<curl_slist*, HeadersCleaner> _headerList;

    std::string              _url;

    std::vector<std::string> _headers;
    std::string              _payload;

    std::string              _cookie;

    std::string              _respBody;
    std::vector<std::string> _respHeaders;

    static std::size_t writeCallback(char *ptr, std::size_t size, size_t nmemb, void *userdata)
    {
        std::string* writeBuffer = (std::string*)userdata;
        std::size_t realsize = size * nmemb;

        (*writeBuffer) += std::string(ptr, realsize);

        return realsize;
     }

    static std::size_t headerCallback(char *ptr, std::size_t size, size_t nmemb, void *userdata)
    {
        std::vector<std::string>* headersBuffer = static_cast<std::vector<std::string>*>(userdata);
        std::size_t realsize = size * nmemb;

        std::size_t headerSize = (realsize >= 2 && ptr[realsize - 2] == '\r' && ptr[realsize - 1] == '\n') ? realsize - 2 : realsize;

        if (headerSize > 0)
        {
            headersBuffer->emplace_back(ptr, headerSize);
        }

        return realsize;
     }
};

} // end of KAFKA_API

