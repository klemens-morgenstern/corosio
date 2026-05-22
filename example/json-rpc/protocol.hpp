//
// Copyright (c) 2026 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//

#ifndef BOOST_COROSIO_EXAMPLE_JSON_RPC_PROTOCOL_HPP
#define BOOST_COROSIO_EXAMPLE_JSON_RPC_PROTOCOL_HPP


#include "libs/json/include/boost/json/fwd.hpp"
#include <boost/capy/task.hpp>
#include <boost/json/value.hpp>

namespace json_rpc
{

// 
struct function_t
{
  using result_t = boost::system::result<boost::json::value>;

  virtual 
  boost::capy::task<result_t> operator()(const boost::json::object &obj) const = 0;
  virtual 
  boost::capy::task<result_t> operator()(const boost::json::array  &arr) const = 0;
  

  virtual ~function_t() = default;
};

namespace detail
{

template<typename T>
boost::system::result<T> find_and_convert(const boost::json::object & obj, std::string_view sv)
{
  auto itr = obj.find(sv);
  if (itr == obj.end())
    return boost::system::error_code(boost::json::error::not_found);

  return boost::json::try_value_to<T>(itr->value());
}

}


template<typename Return, typename ... Args, std::size_t ... N>
auto make_function(Return (*func)(Args ...) noexcept, const char (& ... names)[N]) -> std::unique_ptr<function_t>
{
  struct func_t : function_t
  {
    Return (*func)(Args...);
    std::array<std::string_view, sizeof...(N)> args;

    func_t(Return (*func)(Args...), const char (& ... names)[N]) : func(func), args(names...) {}

    boost::capy::task<result_t> operator()(const boost::json::object &obj) const override
    {
      result_t res = nullptr;

      auto prepped_args = std::apply(
        [&](const auto & ... Ns)
        {
          return std::make_tuple(find_and_convert(obj, Ns)...);
        }, args);

      auto failed = std::apply(
        [](const auto & ... arg) {return (arg.has_error() || ...);},
        prepped_args);

      if (failed)
        co_return boost::json::value{
                {"jsonrpc", "2.0"},
                {"error", {{"code", -32602}, {"message", "Invalid params"}}}
                };


      if constexpr (std::is_void_v<Return>)
      {
        std::apply(
          [&](auto && ... args) { func(*std::move(args)...); },
          std::move(prepped_args));
        co_return boost::json::value{{"jsonrpc", "2.0"}, {"result", nullptr}};
      }
      else
      {
        const auto res = std::apply(
          [&](auto && ... args) { return func(*std::move(args)...); },
          std::move(prepped_args));
        co_return boost::json::value{
              {"jsonrpc", "2.0"}, 
              {"result", boost::json::value_from(std::move(res))}};      
      }
    }
    boost::capy::task<result_t> operator()(const boost::json::array &arr) const override 
    {
      result_t res = nullptr;

      auto prepped_args = std::apply(
        [&](const auto & ... Ns)
        {
          
          return std::make_tuple(find_and_convert(arr, Ns)...);
        }, args);

      auto failed = std::apply(
        [](const auto & ... arg) {return (arg.has_error() || ...);},
        prepped_args);

      if (failed)
        co_return boost::json::value{
                {"jsonrpc", "2.0"},
                {"error", {{"code", -32602}, {"message", "Invalid params"}}}
                };


      if constexpr (std::is_void_v<Return>)
      {
        std::apply(
          [&](auto && ... args) { func(*std::move(args)...); },
          std::move(prepped_args));
        co_return boost::json::value{{"jsonrpc", "2.0"}, {"result", nullptr}};
      }
      else
      {
        const auto res = std::apply(
          [&](auto && ... args) { return func(*std::move(args)...); },
          std::move(prepped_args));
        co_return boost::json::value{
              {"jsonrpc", "2.0"}, 
              {"result", boost::json::value_from(std::move(res))}};      
      }
    
    }
  };

  return std::make_unique<func_t>(func, names...
    );
}


}

#endif // BOOST_COROSIO_EXAMPLE_JSON_RPC_PROTOCOL_HPP
