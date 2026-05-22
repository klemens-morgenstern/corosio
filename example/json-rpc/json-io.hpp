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
#include <boost/capy/error.hpp>
#include <boost/capy/io/any_read_source.hpp>
#include <boost/capy/io/any_write_sink.hpp>
#include <boost/capy/buffers/circular_dynamic_buffer.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/ex/strand.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <memory>

using namespace boost;

namespace json_rpc
{

struct json_reader
{
  json_reader(capy::any_read_source source) : source_(std::move(source)) {}

  capy::task<boost::system::result<json::value>> operator co_await();

 private:

  boost::system::error_code  parse_data_();
 

  capy::task<json::value> read_one_();
  capy::any_read_source source_;
  json::stream_parser sparser_;
  char buffer_[16*16];
  capy::circular_dynamic_buffer cbuffer_{buffer_, sizeof(buffer_)};
};

struct json_writer : std::enable_shared_from_this<json_writer>
{
  json_writer(capy::any_executor exec, capy::any_write_sink sink) : sink_(std::move(sink)), strand_(std::move(exec)) {}

  struct await_completion
  {
    std::optional<capy::task<void>> & worker;
    
    bool await_ready() const 
    {
      return !worker;
    }

    template<typename ... Args>
    auto await_suspend(Args && ... args)
    {
      return worker->await_suspend(std::forward<Args>(args)...);
    }

    auto await_resume()
    {
      if (worker)
          worker->await_resume();
    }
  };

  void write(json::value value);
  await_completion operator co_await () {return await_completion{worker_}; }

 private:

  void launch_write_();
  capy::task<void> do_write_();
 
  std::optional<capy::task<void>> worker_;
  capy::any_write_sink sink_;
  std::exception_ptr ep_;
  std::shared_mutex mtx_;
  std::stop_source ss_;
  std::string active_buffer_, serializing_buffer_;
  capy::strand<capy::any_executor> strand_;
};

}

