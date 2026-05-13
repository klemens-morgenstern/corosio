//
// Copyright (c) 2026 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//

#include "boost/capy/buffers/buffer_array.hpp"
#include "boost/capy/io/any_read_stream.hpp"
#include <boost/capy/buffers.hpp>
#include <boost/capy/asio/boost.hpp>
#include <boost/capy/io/any_buffer_sink.hpp>
#include <boost/capy/io/any_buffer_source.hpp>
#include <boost/capy/io/any_stream.hpp>

#include <boost/capy/io_task.hpp>
#include <boost/capy/buffers/vector_dynamic_buffer.hpp>
#include <boost/beast/http.hpp>
#include <cstddef>

namespace cobeast
{

namespace capy = boost::capy;
namespace http
{

// Adapts a span of capy buffers (`From`) into an asio-compatible
// buffer sequence yielding `To` (e.g. asio::const_buffer /
// mutable_buffer). Beast's parsers and serializers consume
// asio buffer sequences, while this library's public API speaks
// in capy buffers; the iterator converts on dereference, so no
// intermediate container is allocated.
template<typename From, typename To>
struct buffer_sequence_adapter
{
    std::span<From> inner;

    struct iterator_type
    {
        using inner_iterator = std::span<From>::iterator;

        using iterator_concept  = std::random_access_iterator_tag;
        using iterator_category = std::random_access_iterator_tag;
        using value_type        = To;
        using difference_type   = std::ptrdiff_t;
        using reference         = value_type;
        using pointer           = void;


        constexpr iterator_type() noexcept = default;
        iterator_type(inner_iterator current) : it_(current) {}

        [[nodiscard]]
        reference operator*() const noexcept {
            return To(
                it_->data(),
                it_->size()
            );
        }

        [[nodiscard]]
        reference operator[](difference_type n) const noexcept {
            auto const& b = it_[n];
            return To(
                b.data(),
                b.size()
            );
        }

        constexpr iterator_type& operator++() noexcept {
            ++it_;
            return *this;
        }

        constexpr iterator_type operator++(int) noexcept {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        constexpr iterator_type& operator--() noexcept {
            --it_;
            return *this;
        }

        constexpr iterator_type operator--(int) noexcept {
            auto tmp = *this;
            --*this;
            return tmp;
        }

        constexpr iterator_type& operator+=(difference_type n) noexcept {
            it_ += n;
            return *this;
        }

        constexpr iterator_type& operator-=(difference_type n) noexcept {
            it_ -= n;
            return *this;
        }

        [[nodiscard]]
        friend constexpr iterator_type
        operator+(iterator_type it, difference_type n) noexcept {
            it += n;
            return it;
        }

        [[nodiscard]]
        friend constexpr iterator_type
        operator+(difference_type n, iterator_type it) noexcept {
            it += n;
            return it;
        }

        [[nodiscard]]
        friend constexpr iterator_type
        operator-(iterator_type it, difference_type n) noexcept {
            it -= n;
            return it;
        }

        [[nodiscard]]
        friend constexpr difference_type
        operator-(iterator_type lhs,
                iterator_type rhs) noexcept {
            return lhs.it_ - rhs.it_;
        }

        [[nodiscard]]
        friend constexpr bool
        operator==(iterator_type lhs,
                iterator_type rhs) noexcept = default;

        [[nodiscard]]
        friend constexpr auto
        operator<=>(iterator_type lhs,
                    iterator_type rhs) noexcept = default;
    private:
        inner_iterator it_;
    };

    iterator_type begin() const {return {inner.begin()};}
    iterator_type   end() const {return {inner.end()};}
};

// Beast Body model for serialization. The body is fed externally
// as a span of capy const_buffers; each call to the serializer
// drains the current `buffers` and signals whether more chunks
// follow via the `eof` flag.
struct const_buffer_body
{
    struct value_type
    {
        bool eof = false;                          // true on the final chunk
        std::span<capy::const_buffer> buffers;     // bytes to emit next
    };


    // Beast queries size() to set Content-Length on the wire.
    // Reporting a concrete size before the producer has signalled
    // eof would lie about the message length, so we only return
    // a value once the caller has marked this as the last chunk.
    // (This entry point is part of beast's internal Body contract
    // and not part of the documented public API.)
    static
    boost::optional<std::uint64_t>
    size(value_type const& v)
    {
        if (v.eof)
            return boost::capy::buffer_size(v.buffers);
        else
            return boost::none;
    }

    // Serializer-side adapter: hands beast the current buffers and
    // returns `more = !eof` so beast knows whether to expect another
    // get() round.
    struct writer
    {
        template<bool isRequest, class Fields>
        explicit
        writer(boost::beast::http::header<isRequest, Fields> &, value_type & v) : v_(v) {}

        void init(boost::system::error_code& ec)
        {
            ec = {};
        }

        using const_buffers_type = buffer_sequence_adapter<boost::capy::const_buffer, boost::asio::const_buffer>;


        boost::optional<std::pair<const_buffers_type, bool>>
            get(boost::system::error_code& ec)
        {
            // Empty + eof means we've fully drained the producer;
            // return none to terminate the serializer loop.
            if ((boost::capy::buffer_size(v_.buffers) == 0 && v_.eof) || ec)
                return boost::none;
            else
                return std::pair(
                    const_buffers_type(v_.buffers),
                    !v_.eof
                    );
        }

        value_type &v_;
    };
};

// Beast Body model for parsing. Body bytes are delivered into the
// caller-supplied span of capy mutable_buffers; `n` reports how many
// were written on the last put(), `done` is set when the parser
// reaches the end of message.
struct mutable_buffer_body
{
    struct value_type
    {
        std::span<capy::mutable_buffer> buffers;   // sink for incoming bytes
        bool done = false;                         // set after end-of-message
        std::size_t n = 0u;                        // bytes written by last put()
    };

    // Parser-side adapter: copies each chunk beast delivers into the
    // caller's mutable buffers and records the byte count. The sink
    // is intentionally bounded — overflow simply stops the copy at
    // the buffer's capacity (the parser will then re-invoke put()).
    struct reader
    {
        template<bool isRequest, class Fields>
        explicit
        reader(boost::beast::http::header<isRequest, Fields> &, value_type & v) : v_(v) {}

        void init(const boost::optional<std::size_t> &, boost::system::error_code& )
        {
        }

        using mutable_buffers_type = buffer_sequence_adapter<boost::capy::mutable_buffer, boost::asio::mutable_buffer>;

        template<class ConstBufferSequence>
        std::size_t
        put(ConstBufferSequence const& buffers, boost::system::error_code&)
        {

            return v_.n = boost::asio::buffer_copy(
                mutable_buffers_type(v_.buffers),
                buffers
            );
        }

        void
        finish(boost::system::error_code&)
        {
        }

        value_type & v_;
    };
};

namespace beast_http = boost::beast::http;

// Concrete beast parser/serializer/header types bound to the Body
// models above. Callers always go through these aliases rather than
// instantiating beast templates directly.
using  request_parser = beast_http:: request_parser<mutable_buffer_body>;
using response_parser = beast_http::response_parser<mutable_buffer_body>;

using  request_serializer = beast_http:: request_serializer<const_buffer_body>;
using response_serializer = beast_http::response_serializer<const_buffer_body>;

using  request_header = beast_http:: request_header<>;
using response_header = beast_http::response_header<>;

// ---------------------------------------------------------------
// Awaitable I/O primitives.
//
// Each pair (request_* / response_*) provides the same operation
// for either direction of an HTTP exchange. They are the bridge
// between beast's synchronous parser/serializer state machines
// and capy's io_task coroutines: the caller awaits them, while
// underneath they pump bytes through the supplied stream until
// beast signals it is satisfied.
// ---------------------------------------------------------------

// Reads bytes from `stream` into `buffer` and feeds the parser
// until the message header is fully parsed.
capy::io_task<> read_header(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer buffer,
    request_parser &parser);

capy::io_task<> read_header(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer buffer,
    response_parser &parser);


// Reads at most one chunk of body bytes into `body_buffer`,
// returning how many bytes were written. Use this when the caller
// wants to control flow chunk-by-chunk.
capy::io_task<std::size_t> read_some(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer buffer,
    response_parser &parser,
    std::span<capy::mutable_buffer> body_buffer);

capy::io_task<std::size_t> read_some(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer buffer,
    request_parser &parser,
    std::span<capy::mutable_buffer> body_buffer);

// Reads the entire remaining body, draining it into `body` until
// the parser reports end-of-message. Returns the total bytes
// delivered to the sink.
capy::io_task<std::size_t> read(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    response_parser &parser,
    capy::any_buffer_sink body);

capy::io_task<std::size_t> read(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    request_parser &parser,
    capy::any_buffer_sink body);


// Drives the serializer until the message header has been fully
// written to the wire.
capy::io_task<> write_header(
    capy::any_write_stream stream,
    response_serializer &serializer);

capy::io_task<> write_header(
    capy::any_write_stream stream,
    request_serializer &serializer);

// Writes one chunk of body bytes through the serializer. Pass
// `eof = true` on the final chunk so the serializer can close
// out the message (set chunk trailer / final length, etc.).
capy::io_task<std::size_t> write_some(
    capy::any_write_stream stream,
    response_serializer &serializer,
    std::span<capy::const_buffer> body,
    bool eof = false);

capy::io_task<std::size_t> write_some(
    capy::any_write_stream stream,
    request_serializer &serializer,
    std::span<capy::const_buffer> body,
    bool eof = false);

// Streams an entire body from `body` to the wire, pulling chunks
// from the source until it is exhausted.
capy::io_task<std::size_t> write(
    capy::any_write_stream stream,
    response_serializer &serializer,
    capy::any_buffer_source body);

capy::io_task<std::size_t> write(
    capy::any_write_stream stream,
    request_serializer &serializer,
    capy::any_buffer_source body);


// Client-side HTTP/1 connection. Owns the underlying transport
// (`next_layer`) plus a reusable read buffer for incoming bytes.
// A typical exchange is:
//
//   auto [ec, s] = co_await client.request(std::move(header));
//   co_await s.write(body);          // optional request body
//   // s.header() returns the response header
//   co_await s.read(response_body);
struct client_connection
{
  capy::any_stream next_layer;
  std::vector<unsigned char> buffer;  // backing storage for the response parser

  client_connection(capy::any_stream next_layer) : next_layer(std::move(next_layer)) {}

  // Per-request handle returned by `request()`. Lets the caller
  // stream the request body out and the response body back over
  // the same connection. Move-only — the embedded unique_ptrs
  // tie its lifetime to a single exchange.
  struct stream
  {
    template<capy::MutableBufferSequence MB>
    capy::io_task<std::size_t> read_some(MB mb)
    {
        mb_ = capy::mutable_buffer_array<8u>(std::move(mb));
        return read_some(mb_.to_span());
    }

    capy::io_task<std::size_t> read_some(std::span<capy::mutable_buffer> mb)
    {
        return http::read_some(
            capy::any_read_stream(&cc_->next_layer), 
            capy::vector_dynamic_buffer(&cc_->buffer), 
            *parser_, mb);
    }


    template<capy::MutableBufferSequence MB>
    capy::io_task<std::size_t> read(MB mb)
    {
        mb_ = capy::mutable_buffer_array<8u>(std::move(mb));
        return read(mb_.to_span());
    }

    capy::io_task<std::size_t> read(std::span<capy::mutable_buffer> mb)
    {
        return http::read_some(
            capy::any_read_stream(&cc_->next_layer), 
            capy::vector_dynamic_buffer(&cc_->buffer), 
            *parser_, mb);
    }


    template<capy::ConstBufferSequence CB>
    capy::io_task<std::size_t> write_some(CB cb)
    {
        cb_ = capy::const_buffer_array<8u>(std::move(cb));
        return write_some(cb_.to_span());
    }
    
    capy::io_task<std::size_t> write_some(std::span<capy::const_buffer> cb)
    {
        return http::write_some(
                capy::any_write_stream(&cc_->next_layer), 
                *serializer_,
                cb);
    }

    template<capy::ConstBufferSequence CB>
    capy::io_task<std::size_t> write(CB cb)
    {
        cb_ = capy::const_buffer_array<8u>(std::move(cb));
        return write(cb_.to_span());
    }
    
    capy::io_task<std::size_t> write(std::span<capy::const_buffer> cb)
    {
        return http::write_some(
                capy::any_write_stream(&cc_->next_layer), 
                *serializer_,
                cb, true);
    }

    template<capy::ConstBufferSequence CB>
    capy::io_task<std::size_t> write_eof(CB cb)
    {
        return write(std::move(cb));
    }

    // Closes out the request body with an empty final chunk.
    // Call this after `write_some()` runs when you have no more
    // payload to send but still need to flush the terminating
    // framing (chunk trailer / final length).
    capy::io_task<> write_eof()
    {
        auto [ec, n] = co_await write(std::span<capy::const_buffer>());
        co_return ec;
    }


    operator bool() const {return cc_ != nullptr;}

    stream() noexcept= default;
    stream(const stream &) noexcept = delete;
    stream(stream &&) noexcept = default;

    stream& operator=(const stream &) noexcept = delete;
    stream& operator=(stream &&) noexcept = default;

    stream(client_connection * cc, std::unique_ptr<request_serializer> ser)
        : cc_(cc), serializer_(std::move(ser))
    {
    }
    const response_header & header()
    {
        return parser_->get();
    }
  private:
    client_connection * cc_ = nullptr;
    std::unique_ptr<request_serializer> serializer_;  // drives the request body out
    std::unique_ptr<response_parser>    parser_;      // collects the response

    // Storage that backs the spans handed to read_some/write_some
    // when the caller passes a typed buffer sequence — keeps the
    // adapter span alive for the duration of the io_task.
    capy::const_buffer_array<8u> cb_;
    capy::mutable_buffer_array<8u> mb_;
  };


  // Sends the request header eagerly and returns a `stream` the
  // caller can use to stream the request body and read back the
  // response. The header write must complete before the caller
  // starts touching the body so beast knows the framing in use.
  capy::io_task<stream> request(request_header h)
  {
    using msg_t = beast_http::message<true, const_buffer_body>;
    auto msg = msg_t(std::move(h));
    auto serializer = std::make_unique<request_serializer>(msg);
    auto [ec] = co_await write_header(
                        capy::any_write_stream(&next_layer),
                        *serializer);
    co_return {ec, stream(this, std::move(serializer))};

  }
};

// Server-side HTTP/1 connection. The exchange is split into two
// half-streams because the server must finish reading the request
// before it begins writing the response (and the two operations
// can interleave only at the body boundary). Typical use:
//
//   auto [ec, r] = co_await server.receive();      // request header
//   co_await r.read(request_body);
//   auto [ec2, w] = co_await server.respond(hdr);  // response header
//   co_await w.write(response_body);
struct server_connection
{
  capy::any_stream next_layer;
  std::vector<unsigned char> buffer;  // backing storage for the request parser

  server_connection(capy::any_stream next_layer) : next_layer(std::move(next_layer)) {}

  // Read half: handed back from `receive()` once the request
  // header is parsed. Used to drain the request body.
  struct read_stream
  {
    template<capy::MutableBufferSequence MB>
    capy::io_task<std::size_t> read_some(MB mb)
    {
        mb_ = capy::mutable_buffer_array<8u>(std::move(mb));
        return read_some(mb_.to_span());
    }

    capy::io_task<std::size_t> read_some(std::span<capy::mutable_buffer> mb)
    {
        return http::read_some(
            capy::any_read_stream(&cc_->next_layer), 
            capy::vector_dynamic_buffer(&cc_->buffer), 
            *parser_, mb);
    }


    template<capy::MutableBufferSequence MB>
    capy::io_task<std::size_t> read(MB mb)
    {
        mb_ = capy::mutable_buffer_array<8u>(std::move(mb));
        return read(mb_.to_span());
    }

    capy::io_task<std::size_t> read(std::span<capy::mutable_buffer> mb)
    {
        return http::read_some(
            capy::any_read_stream(&cc_->next_layer), 
            capy::vector_dynamic_buffer(&cc_->buffer), 
            *parser_, mb);
    }

    operator bool() const {return cc_ != nullptr;}
    
    read_stream() noexcept= default;
    read_stream(const read_stream &) noexcept = delete;
    read_stream(read_stream &&) noexcept = default;

    read_stream& operator=(const read_stream &) noexcept = delete;
    read_stream& operator=(read_stream &&) noexcept = default;
    
    read_stream(server_connection * cc, std::unique_ptr<request_parser> parser = nullptr) 
        : cc_(cc), parser_(std::move(parser))
    {
    }
    const request_header & header() 
    {
        return parser_->get();
    }
  private:
    server_connection * cc_ = nullptr;
    std::unique_ptr<request_parser> parser_;
    capy::mutable_buffer_array<8u> mb_;
  };

  // Write half: handed back from `respond()` once the response
  // header has been written. Used to stream the response body.
  struct write_stream
  {
    template<capy::ConstBufferSequence CB>
    capy::io_task<std::size_t> write_some(CB cb)
    {
        cb_ = capy::const_buffer_array<8u>(std::move(cb));
        return write_some(cb_.to_span());
    }
    
    capy::io_task<std::size_t> write_some(std::span<capy::const_buffer> cb)
    {
        return http::write_some(
                capy::any_write_stream(&cc_->next_layer), 
                *serializer_,
                cb);
    }

    template<capy::ConstBufferSequence CB>
    capy::io_task<std::size_t> write(CB cb)
    {
        cb_ = capy::const_buffer_array<8u>(std::move(cb));
        return write(cb_.to_span());
    }
    
    capy::io_task<std::size_t> write(std::span<capy::const_buffer> cb)
    {
        return http::write_some(
                capy::any_write_stream(&cc_->next_layer), 
                *serializer_,
                cb, true);
    }

    template<capy::ConstBufferSequence CB>
    capy::io_task<std::size_t> write_eof(CB cb)
    {
        return write(std::move(cb));
    }

    // Final flush — same role as on the client-side stream:
    // emits an empty terminating chunk so beast can close out
    // the response framing.
    capy::io_task<> write_eof()
    {
        auto [ec, n] = co_await write(std::span<capy::const_buffer>());
        co_return ec;
    }


    operator bool() const {return cc_ != nullptr;}

    write_stream() noexcept= default;
    write_stream(const write_stream &) noexcept = delete;
    write_stream(write_stream &&) noexcept = default;

    write_stream& operator=(const write_stream &) noexcept = delete;
    write_stream& operator=(write_stream &&) noexcept = default;
    
    write_stream(server_connection * cc, std::unique_ptr<response_serializer> serializer) 
        : cc_(cc), serializer_(std::move(serializer))
    {
    }
    
  private:
    server_connection * cc_ = nullptr;
    std::unique_ptr<response_serializer> serializer_;
    
    capy::const_buffer_array<8u> cb_;
  };

    template<typename ...>auto foo();

  // Reads the next request header from the connection and returns
  // a `read_stream` positioned to drain its body. Will yield until
  // a complete header has arrived.
  capy::io_task<read_stream> receive()
  {
    auto parser = std::make_unique<request_parser>();
    auto [ec] = co_await read_header(
                        capy::any_read_stream(&next_layer),
                        capy::vector_dynamic_buffer(&buffer),
                        *parser);
    co_return {ec, read_stream(this, std::move(parser))};
  }

  // Sends the response header eagerly and returns a `write_stream`
  // for the body. Must be called only after the matching request
  // (or at least its header) has been received, otherwise the
  // peer will see responses with no request to correlate them.
  capy::io_task<write_stream> respond(response_header h)
  {
    using msg_t = beast_http::message<false, const_buffer_body>;
    auto msg = msg_t(std::move(h));
    auto serializer = std::make_unique<response_serializer>(msg);
    auto [ec] = co_await write_header(
                        capy::any_write_stream(&next_layer),
                        *serializer);
    co_return {ec, write_stream(this, std::move(serializer))};

  }
};

    
}

}
