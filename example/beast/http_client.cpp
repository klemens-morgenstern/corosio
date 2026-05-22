//
// Copyright (c) 2026 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//

// HTTP/1.1 GET client built on the cobeast::http API in beast_core.hpp.
//
// Notes on the API as it stands today (avoided rather than fixed here):
//   * client_connection::request() builds a local beast message and stores
//     a serializer that references it — the message is destroyed when
//     request() returns, leaving the serializer dangling. This example
//     therefore owns the message in `do_request` and calls the free
//     write_header / write_some functions directly.
//   * client_connection::stream cannot receive responses today (its
//     response_parser is never created), so response handling also goes
//     through the free read_header / read functions.
//   * read_some is declared by-value in the header but defined by-ref in
//     the .cpp, so it would not link from outside the .cpp — we stick to
//     read_header + read which match across header/impl.

#include "beast.hpp"
#include <boost/corosio/openssl_stream.hpp>
#include <boost/corosio/resolver.hpp>
#include <boost/url/scheme.hpp>
#include <boost/url/url.hpp>

#include <boost/corosio.hpp>
#include <boost/corosio/connect.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/buffers/make_buffer.hpp>
#include <boost/capy/error.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/io/any_buffer_sink.hpp>
#include <boost/capy/io_task.hpp>
#include <boost/capy/task.hpp>
#include <boost/url/parse.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>

namespace corosio   = boost::corosio;
namespace capy      = boost::capy;
namespace beast_http = boost::beast::http;

capy::task<int>
do_request(
    std::string_view host,
    std::string_view service,
    bool is_https,
    std::string_view target)
{
    corosio::resolver res{co_await capy::this_coro::executor};
    auto [ec, ep] = co_await res.resolve(host, service);

    if (ec || ep.empty())
    {
        std::cerr << "Could not resolve '" << host << "': " << ec << "\n";
        co_return 1;
    }

    capy::any_stream sock;
    corosio::tls_context ctx;
    
    if (is_https)
    {
        std::ignore = ctx.set_default_verify_paths().value();
        std::ignore = ctx.set_verify_mode(corosio::tls_verify_mode::peer).value();
        ctx.set_hostname("host");

        corosio::tcp_socket tsock{co_await capy::this_coro::executor};
        ec = get<0>(co_await corosio::connect(tsock, ep));
        
        if (!ec)
        {
            corosio::openssl_stream os{std::move(tsock), ctx};
            ec = get<0>(co_await os.handshake(corosio::tls_stream::client));            
            sock = std::move(os);        
        }        
    }
    else
    {
        corosio::tcp_socket tsock{co_await capy::this_coro::executor};
        ec = get<0>(co_await corosio::connect(tsock, ep));
        sock = std::move(tsock);
    }

    if (ec)
    {
        std::cerr << "Could not connect to '" << host << "': " << ec << "\n";
        co_return 1;
    }


    cobeast::http::request_header h;
    h.method(beast_http::verb::get);
    h.target(std::string(target));
    h.version(11);
    h.set(beast_http::field::host, std::string(host));
    h.set(beast_http::field::user_agent, "corosio-beast-example");
    h.set(beast_http::field::connection, "close");

    cobeast::http::client_connection conn{std::move(sock)};

    auto [ec_, s] = co_await conn.request(std::move(h));
    ec_ = ec;
    if (!ec)
        ec = get<0>(co_await s.write_eof());

    if (ec)
    {
        std::cerr << "Error sending request '" << host << "': " << ec << "\n";
        co_return 1;
    }
    
    
    std::size_t read = 0u; 
    std::string body;
    
    while (!ec)
    {
        body.resize(65535);
        auto buf = capy::make_buffer(body);
        
        buf += read;
        auto [ec_, n] = co_await s.read_some(buf);
        ec = ec_;
        read += n;
        body.resize(read);
    }

    std::cout << body;
    if (!body.empty() && body.back() != '\n')
        std::cout << '\n';

    co_return 0;
}

int
main(int argc, char* argv[])
{
    if (argc < 2)
    {
        std::cerr <<
            "Usage: http_client <url>\n"
            "Example:\n"
            "    http_client http://example.com:80/\n";
        return 1;
    }

    auto url = boost::urls::parse_uri(argv[1]);

    if (!url)
    {
      std::cerr << "Invalid url: " 
                <<  url.error().to_string() << "\n";
      return 1;
    }

    corosio::io_context ioc;

    int res = 0;

    capy::run_async(
        ioc.get_executor(),
        [&](int i){res = i;}
        )(
        do_request(
            url->host(), 
            url->has_port() 
                ? url->port() 
                : (url->has_scheme() ? url->scheme() : "https"), 
            url->scheme_id() == boost::urls::scheme::https,
            url->encoded_target()));
    
    ioc.run();

    return res;
}
