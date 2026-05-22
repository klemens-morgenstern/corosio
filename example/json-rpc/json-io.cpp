#include "json-io.hpp"


//
// Copyright (c) 2026 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//

// first, create a json class that reads jsons from the 


#include <boost/capy/ex/any_executor.hpp>
#include <boost/json.hpp>
#include <boost/json/src.hpp>

#include <boost/capy/error.hpp>
#include <boost/capy/io/any_read_source.hpp>
#include <boost/capy/io/any_write_sink.hpp>
#include <boost/capy/buffers/circular_dynamic_buffer.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/ex/strand.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <memory>
#include <mutex>

using namespace boost;

namespace json_rpc
{

capy::task<boost::system::result<json::value>> json_reader::operator co_await()
{
  while (!sparser_.done())
  {
    if (auto ec = parse_data_())
      co_return ec;
      
    if (!sparser_.done())
    {
      auto [ec, n] = 
        co_await source_.read(cbuffer_.prepare(cbuffer_.capacity()));
      cbuffer_.commit(n);
      
      if (ec == capy::error::eof) // 
      {
        ec.clear();
        ec = parse_data_();
        if (!ec)
          sparser_.finish(ec);
      }
      
      if (ec)
        co_return ec;
    }          
  }

  co_return sparser_.release();
}

boost::system::error_code json_reader::parse_data_()
{
  boost::system::error_code ec;
  while (!cbuffer_.data().empty() && !sparser_.done() && !ec)
  {
    auto f = cbuffer_.data().front();
    auto n = sparser_.write_some(
                      static_cast<const char*>(f.data()), f.size(), ec);
    cbuffer_.consume(n);
  }
  return ec;
}
 

void json_writer::write(json::value value)
{
  std::unique_lock lg{mtx_, std::defer_lock};
  json::serializer sr;
  sr.reset(&value);

  while (!sr.done())
  {
    char buffer[4096];
    auto sv = sr.read(buffer);
    if (!lg.owns_lock())
      lg.lock();
    serializing_buffer_.append(sv.data(), sv.size());
  }

  // we always own the lock here
  if (active_buffer_.empty())
  {
    std::swap(active_buffer_, serializing_buffer_);
    launch_write_();
  }
}


void json_writer::launch_write_()
{  
    capy::run_async(
      strand_, 
      ss_.get_token()
    )(do_write_());
}

capy::task<void> json_writer::do_write_()
{
  auto p = this->shared_from_this();
  std::size_t offset = 0u;

  capy::const_buffer buffer(active_buffer_.data(), active_buffer_.size());

  while (buffer.size() > 0 && ss_.stop_requested())
  {
    auto [ec, n ] = co_await sink_.write(buffer);
    buffer += n;

    if (buffer.size() == 0u)
    {
      std::lock_guard lg{mtx_};
      active_buffer_.clear();
      if (!serializing_buffer_.empty())
        std::swap(active_buffer_, serializing_buffer_);
      
      buffer = capy::const_buffer(active_buffer_.data(), active_buffer_.size());
    }
  }

  if (ss_.stop_requested())
    std::ignore =  co_await sink_.write_eof();
}
 
}


