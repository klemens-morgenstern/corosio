//
// Copyright (c) 2026 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//


#ifndef BOOST_COROSIO_EXAMPLE_JSON_RPC_METHOD_HPP
#define BOOST_COROSIO_EXAMPLE_JSON_RPC_METHOD_HPP

#include <boost/callable_traits.hpp>
#include <boost/json.hpp>
#include <initializer_list>
#include <memory>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <utility>

#include "boost/callable_traits/return_type.hpp"
#include "boost/json/fwd.hpp"
#include "error.hpp"

namespace json_rpc
{


// mapping of error_categories?

struct method_base 
{
  virtual boost::json::value invoke(boost::json::value params) = 0;

  virtual ~method_base() = default;
};

template<typename Func>
struct method final : method_base
{
  using args_t = boost::callable_traits::args_t<Func>;
  using return_t = boost::callable_traits::return_type_t<Func>;
  
  Func func;
  std::array<std::string_view, std::tuple_size_v<args_t>> names;

  method(Func && func, std::initializer_list<std::string_view> names) 
    : func(std::move(func))
  {
    std::copy(names.begin(), names.end(), this->names.begin(), this->names.end());
  }

  static boost::json::value make_error(error e)
  {
    return 
      {
          {"jsonrpc", "2.0"},
          {"error", 
            {
              {"code", static_cast<int>(e)},
              {"message", json_rpc_error.message(static_cast<int>(e))}
            }
          }
        };
  }

  
  static boost::json::value make_error(std::string_view message)
  {
    return 
      {
          {"jsonrpc", "2.0"},
          {"error", 
            {
              {"code", static_cast<int>(error::internal_error)},
              {"message", message}
            }
          }
        };
  }

  template<typename ... Ts>
  boost::json::value do_invoke(boost::system::result<Ts> ... args)
  try 
  {
    const auto args_valid = (args && ...);
    if (!args_valid)
      return make_error(error::invalid_params);

    if constexpr (std::is_void_v<return_t>)
    {
      func(*std::move(args)...);
      return {{"jsonrpc", "2.0"}, {"result", nullptr}};
    }
    else
      return {{"jsonrpc", "2.0"}, {"result", 
              boost::json::value_from(func(*std::move(args)...))}};
  }
  catch (std::system_error & se)
  {
    if (se.code().category() == json_rpc_error)
      return make_error(se.code());
    else 
      return make_error(se.what());
  }
  catch (std::exception & ex)
  {
    return make_error(ex.what());
  }
  
  template<std::size_t Idx> 
  boost::system::result<std::tuple_element_t<Idx, args_t>> 
      get_object_param(const boost::json::object & obj)
  {
    auto itr = obj.find(names[Idx]);
    if (itr == obj.end())
      return boost::json::error::not_found;

    using tt = std::tuple_element_t<Idx, args_t>;
    return boost::json::try_value_to<tt>(itr->second);
  }

  template<std::size_t ... Idx>
  boost::json::value invoke_object(const boost::json::object& obj, 
                                   std::index_sequence<Idx...>)
  {
    return do_invoke(get_object_param<Idx>(obj)...);
  }

  template<std::size_t ... Idx>
  boost::json::value invoke_array(const boost::json::array& arr, 
                                   std::index_sequence<Idx...>)
  {
    return do_invoke(
        boost::json::try_value_to<
          std::tuple_element_t<Idx, args_t>>(arr[Idx])...);
  }

  boost::json::value invoke(boost::json::value params) final
  {
    if (auto obj = params.if_object())
      return invoke_object(*obj, std::make_index_sequence<names.size()>());
    else if (auto arr = params.if_array(); 
             arr && arr->size() == std::tuple_size_v<args_t>)
      return invoke_array(*obj, std::make_index_sequence<names.size()>());
    else
      return make_error(error::invalid_params);
  }
};

using method_ptr = std::unique_ptr<method_base>;

template<typename Func>
auto make_method(Func &&func, std::initializer_list<std::string_view> args) 
  -> method_ptr
{
  using m_t = method<std::remove_reference_t<Func>>;
   return std::make_unique<m_t>(std::forward<Func>(func), args);
}


} 

#endif

