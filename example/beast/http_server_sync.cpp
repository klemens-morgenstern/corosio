//
// Copyright (c) 2026 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//

// Coroutine port of Boost.Beast's example/http/server/sync example,
// built on the cobeast::http::server_connection API in beast_core.hpp
// and corosio's tcp_server worker pool.
//
// Each worker wraps its accepted socket in a server_connection, reads
// the request header through receive(), composes a plain-text reply,
// and writes it back. The exchange always sets Connection: close, so
// the server closes the socket after one round-trip — matching the
// shape of beast's sync server and avoiding the need to drain request
// bodies for pipelined follow-up.
//
// server_connection::respond() is *not* used here: it returns a
// write_stream whose response_serializer references a message owned
// by the respond() coroutine frame (already destroyed by the time
// the caller observes the result), and the cobeast write helpers
// have an internal cap on buffer-sequence fan-out that chunked
// transfer encoding overflows. Instead, the response message is
// owned in this frame with `body().eof = true` set up front — beast
// then knows the body size, emits Content-Length-delimited framing
// (smaller buffer fan-out, no chunk_crlf), and we drive the wire
// through the free write_header / write_some functions.

#include "beast.hpp"

#include <boost/corosio.hpp>
#include <boost/corosio/tcp_server.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/io/any_stream.hpp>
#include <boost/capy/io_task.hpp>
#include <boost/capy/task.hpp>
#include <boost/capy/write.hpp>
#include <boost/beast/version.hpp>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace corosio    = boost::corosio;
namespace capy       = boost::capy;
namespace beast_http = boost::beast::http;

namespace {

// Build a 200 OK response header. Content-Length is set
// automatically by beast from the body size we install on the
// message (see do_session).
cobeast::http::response_header
make_response_header(int version)
{
    cobeast::http::response_header h;
    h.version(version);
    h.result(beast_http::status::ok);
    h.set(beast_http::field::content_type, "text/plain");
    h.set(beast_http::field::connection, "close");
    return h;
}

class http_worker : public corosio::tcp_server::worker_base
{
    corosio::io_context & ctx_;
    corosio::tcp_socket sock_;

public:
    explicit http_worker(corosio::io_context & ctx)
        : ctx_(ctx)
        , sock_(ctx)
    {
    }

    corosio::tcp_socket & socket() override { return sock_; }

    void run(corosio::tcp_server::launcher launch) override
    {
        launch(ctx_.get_executor(), do_session());
    }

    capy::task<void> do_session()
    {
        try
        {
            cobeast::http::server_connection cc{
                capy::any_stream(&sock_)};

            // receive() reads the request header and hands back a
            // read_stream owning a heap-allocated request_parser
            // (no dangling-reference issue on this side).
            auto [rec, r] = co_await cc.receive();
            if (rec)
                co_return;

            auto const & req = r.header();
            std::string body;
            body += "Hello from corosio + cobeast!\n";
            body += "Method: ";
            body += std::string(req.method_string());
            body += '\n';
            body += "Target: ";
            body += std::string(req.target());
            body += '\n';

            capy::const_buffer body_buf(body.data(), body.size());

            // Own the message in this frame, pre-arm the body with
            // the bytes we're about to send and eof=true so beast
            // can compute Content-Length at header time and pick
            // non-chunked framing.
            beast_http::message<
                false, cobeast::http::const_buffer_body> msg(
                    make_response_header(req.version()));
            msg.body().buffers =
                std::span<capy::const_buffer>(&body_buf, 1);
            msg.body().eof = true;

            cobeast::http::response_serializer ser(msg);

            if (auto [ec] = co_await cobeast::http::write_header(
                    capy::any_write_stream(&cc.next_layer), ser); ec)
                co_return;

            auto [wec, wn] = co_await cobeast::http::write_some(
                capy::any_write_stream(&cc.next_layer), ser,
                std::span<capy::const_buffer>(&body_buf, 1),
                /*eof=*/true);
            (void)wec;
            (void)wn;
        }
        catch (...)
        {
            // Swallow per-connection errors so a single bad client
            // can't take the worker out of rotation.
        }

        if (sock_.is_open())
            sock_.close();
    }
};

inline auto
make_workers(corosio::io_context & ctx, int n)
{
    std::vector<std::unique_ptr<corosio::tcp_server::worker_base>> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i)
        v.push_back(std::make_unique<http_worker>(ctx));
    return v;
}

class http_server : public corosio::tcp_server
{
public:
    http_server(corosio::io_context & ctx, int max_workers)
        : tcp_server(ctx, ctx.get_executor())
    {
        set_workers(make_workers(ctx, max_workers));
    }
};

} // namespace

int
main(int argc, char* argv[])
{
    if (argc != 3)
    {
        std::cerr <<
            "Usage: http_server_sync <port> <max-workers>\n"
            "Example:\n"
            "    http_server_sync 8080 10\n";
        return EXIT_FAILURE;
    }

    int port_int = std::atoi(argv[1]);
    if (port_int <= 0 || port_int > 65535)
    {
        std::cerr << "Invalid port: " << argv[1] << '\n';
        return EXIT_FAILURE;
    }
    auto port = static_cast<std::uint16_t>(port_int);

    int max_workers = std::atoi(argv[2]);
    if (max_workers <= 0)
    {
        std::cerr << "Invalid max-workers: " << argv[2] << '\n';
        return EXIT_FAILURE;
    }

    corosio::io_context ioc;
    http_server server(ioc, max_workers);

    if (auto ec = server.bind(corosio::endpoint(port)); ec)
    {
        std::cerr << "Bind failed: " << ec.message() << '\n';
        return EXIT_FAILURE;
    }

    std::cout << "HTTP server listening on port " << port
              << " with " << max_workers << " workers\n";

    server.start();
    ioc.run();

    return EXIT_SUCCESS;
}
