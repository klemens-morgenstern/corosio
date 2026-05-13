//
// Copyright (c) 2026 Vinnie Falco (vinnie.falco@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//

#include "beast_core.hpp"
#include "boost/capy/asio/buffers.hpp"
#include <boost/capy/buffers.hpp>
#include <boost/capy/buffers/circular_dynamic_buffer.hpp>
#include <boost/capy/concept/stream.hpp>
#include <boost/capy/io/any_stream.hpp>
#include <boost/capy/write.hpp>
#include <boost/asio/buffers_iterator.hpp>

#include <system_error>

namespace cobeast::http
{

template<typename Parser>
capy::io_task<> read_header_impl(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer buffer, 
    Parser & parser)
{
  parser.eager(false);
    
  while (!parser.is_header_done())
  {

    boost::system::error_code sec = boost::beast::http::error::need_more;
    while (sec == boost::beast::http::error::need_more)
    {
      const auto bf =  buffer.prepare(65535);
      const auto [ec, n] = co_await stream.read_some(bf);
      buffer.commit(n);
      if (ec)
        co_return {ec};

      const auto m = parser.put(boost::asio::const_buffer(bf.data(), n), sec);
      buffer.consume(m);
      if (sec)
        co_return {std::error_code(sec)};
    }
  }
  co_return {};
}

capy::io_task<> read_header(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer buffer, 
    request_parser &parser)
{
    return read_header_impl(std::move(stream), std::move(buffer), parser);
}


capy::io_task<> read_header(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer buffer, 
    response_parser &parser)
{
    return read_header_impl(std::move(stream), std::move(buffer), parser);
}

template<typename Parser>
capy::io_task<std::size_t> read_some_impl(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    Parser &parser,
    std::span<capy::mutable_buffer> body)
{
  parser.eager(true);
   
  if (parser.is_done())
  {
    co_return {capy::error::eof, 0u};
  }

  std::size_t m = 0u;

  boost::system::error_code sec = boost::beast::http::error::need_more;
  while (sec == boost::beast::http::error::need_more)
  {

    const auto bf = buffer.prepare(65535);
    auto [ec, n] = co_await stream.read_some(bf);
    buffer.commit(n);

    if (ec)
      co_return {ec, 0u};

    char body_buffer[65535] = {};
    boost::capy::mutable_buffer cb{&body_buffer, sizeof(body_buffer)};

    parser.get().body().buffers = body;
    n = parser.put(boost::asio::const_buffer(bf.data(), n), sec);
    
    if (sec && sec != boost::beast::http::error::need_more)
        ec = sec;
        
    if (ec)
        co_return {ec, m};
  }

  co_return {{}, m};
}


capy::io_task<std::size_t> read_some(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    response_parser &parser,
    std::span<capy::mutable_buffer> body)
{
    return read_some_impl(
            std::move(stream),
            buffer, parser,
            body
            );
}


capy::io_task<std::size_t> read_some(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    request_parser &parser,
    std::span<capy::mutable_buffer> body)
{
    return read_some_impl(
            std::move(stream),
            buffer, parser,
            body
            );
}



template<typename Parser>
capy::io_task<std::size_t> read_impl(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    Parser &parser,
    capy::any_buffer_sink body)
{
  parser.eager(true);
   
  if (parser.is_done())
  {
    auto [ec] = co_await body.commit_eof(0u);
    co_return {ec, 0u};
  }

  std::size_t m = 0u;

  boost::system::error_code sec = boost::beast::http::error::need_more;
  while (sec == boost::beast::http::error::need_more || !parser.is_done())
  {

    const auto bf = buffer.prepare(65535);
    auto [ec, n] = co_await stream.read_some(bf);
    buffer.commit(n);

    if (ec)
      co_return {ec, 0u};

    char body_buffer[65535] = {};
    boost::capy::mutable_buffer cb{&body_buffer, sizeof(body_buffer)};

    parser.get().body().buffers 
        = body.prepare(std::span<boost::capy::mutable_buffer>(&cb, 1u));


    n = parser.put(boost::asio::const_buffer(bf.data(), n), sec);
    

    if (parser.is_done())
        ec = get<0>(co_await body.commit_eof(parser.get().body().n));
    else
        ec = get<0>(co_await body.commit(parser.get().body().n));

    if (sec && sec != boost::beast::http::error::need_more)
        ec = sec;
        
    if (ec)
        co_return {ec, m};
  }

  co_return {{}, m};
}

capy::io_task<std::size_t> read(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    response_parser &parser,
    capy::any_buffer_sink body)
{
    return read_impl(
            std::move(stream),
            buffer, parser,
            std::move(body)
            );
}

capy::io_task<std::size_t> read(
    capy::any_read_stream stream,
    capy::vector_dynamic_buffer & buffer,
    request_parser &parser,
    capy::any_buffer_sink body)
{
    return read_impl(
            std::move(stream),
            buffer, parser,
            std::move(body)
            );
}

template<typename Serializer>
capy::io_task<> write_header_impl(
    capy::any_write_stream stream,
    Serializer &serializer)
{
  serializer.split(true);

  std::array<boost::capy::const_buffer, 8u> buffer;
  std::span<boost::capy::const_buffer> spn;
    
  boost::system::error_code ec;
  serializer.next(
      ec, 
      [&](boost::system::error_code &, auto cb) 
      {
        // max size is 8
        auto b = boost::asio::buffer_sequence_begin(cb), 
             e = boost::asio::buffer_sequence_end(cb);
        assert(std::distance(b, e) < 8);
        const auto it = std::transform(
                b, e, buffer.begin(),
                [&](boost::asio::const_buffer cb) -> boost::capy::const_buffer
                {
                    return {cb.data(), cb.size()};
                        });

        spn = {buffer.begin(), it};
      }
    );

  if (spn.empty())
    co_return {ec};

  co_return get<0>(co_await boost::capy::write(stream, spn));
}


capy::io_task<> write_header(
    capy::any_write_stream stream,
    request_serializer &serializer)
{
  return write_header_impl(std::move(stream), serializer);
}


capy::io_task<> write_header(
    capy::any_write_stream stream,
    response_serializer &serializer)
{
  return write_header_impl(std::move(stream), serializer);
}


template<typename Serializer>
capy::io_task<std::size_t> write_some_impl(
    capy::any_write_stream stream,
    Serializer &serializer,
    std::span<capy::const_buffer> body,
    bool eof)
{
  serializer.split(false);

  std::array<boost::capy::const_buffer, 8u> buffer;
  std::span<boost::capy::const_buffer> spn;
  
  const_buffer_body::value_type & bd = serializer.get().body();

  char body_buffer[65535] = {};
  boost::capy::const_buffer cb{&body_buffer, sizeof(body_buffer)};

  
  serializer.get().body().buffers = body;
  serializer.get().body().eof = eof;
  
  
  boost::system::error_code sec;
  serializer.next(
      sec, 
      [&](boost::system::error_code &, auto cb) 
      {
        // max size is 8
        auto b = boost::asio::buffer_sequence_begin(cb), 
             e = boost::asio::buffer_sequence_end(cb);
        assert(std::distance(b, e) < 8);
        const auto it = std::transform(
                b, e, buffer.begin(),
                [&](boost::asio::const_buffer cb) -> boost::capy::const_buffer
                {
                    return {cb.data(), cb.size()};
                });

        spn = {buffer.begin(), it};
      }
    );

  if (spn.empty())
    co_return {sec, 0u};

  auto r = co_await boost::capy::write(stream, spn);
  co_return r;
}

capy::io_task<std::size_t> write_some(
    capy::any_write_stream stream,
    request_serializer &serializer,
    std::span<capy::const_buffer> body,
    bool eof)
{
    return write_some_impl(std::move(stream), serializer, body, eof);
}

capy::io_task<std::size_t> write_some(
    capy::any_write_stream stream,
    response_serializer &serializer,
    std::span<capy::const_buffer> body,
    bool eof)
{
    return write_some_impl(std::move(stream), serializer, body, eof);
}


template<typename Serializer>
capy::io_task<std::size_t> write_impl(
    capy::any_write_stream stream,
    Serializer &serializer,
    capy::any_buffer_source body)
{

  std::array<boost::capy::const_buffer, 8u> buffer;
  std::span<boost::capy::const_buffer> spn;
  
  const_buffer_body::value_type & bd = serializer.get().body();

  char body_buffer[65535] = {};
  boost::capy::const_buffer cb{&body_buffer, sizeof(body_buffer)};

  std::error_code ec;
  std::size_t w = 0u;
 
  while (!ec)
  {
    auto [ec_, bfs] = co_await body.pull({&cb, 1u});
    ec = ec_;


    if (ec && ec != boost::capy::error::eof)
      co_return {ec, 0u};
    
    serializer.get().body().buffers = bfs;
    serializer.get().body().eof = true;
    
    boost::system::error_code sec;
    serializer.next(
        sec, 
        [&](boost::system::error_code &, auto cb) 
        {
          // max size is 8
          auto b = boost::asio::buffer_sequence_begin(cb), 
               e = boost::asio::buffer_sequence_end(cb);
          assert(std::distance(b, e) < 8);
          const auto it = std::transform(
                  b, e, buffer.begin(),
                  [&](boost::asio::const_buffer cb) -> boost::capy::const_buffer
                  {
                        return {cb.data(), cb.size()};
                    });
  
          spn = {buffer.begin(), it};
        }
      );
  
      if (spn.empty())
        co_return {sec, w};
    
      auto [ee, m] = co_await boost::capy::write(stream, spn);
      ec = ee;
      body.consume(m);
      w += m;
    }
    co_return {ec, w};
}

capy::io_task<std::size_t> write(
    capy::any_write_stream stream,
    request_serializer &serializer,
    capy::any_buffer_source body)
{
    return write_impl(std::move(stream), serializer, std::move(body));
}

capy::io_task<std::size_t> write(
    capy::any_write_stream stream,
    response_serializer &serializer,
    capy::any_buffer_source body)
{
    return write_impl(std::move(stream), serializer, std::move(body));
}


static_assert(boost::capy::Stream<typename client_connection::stream>);
static_assert(boost::capy::ReadSource<typename client_connection::stream>);
static_assert(boost::capy::WriteSink<typename client_connection::stream>);
static_assert(boost::capy::ReadSource<typename server_connection::read_stream>);
static_assert(boost::capy::WriteSink<typename server_connection::write_stream>);

}

