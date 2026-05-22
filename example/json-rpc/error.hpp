//
// Copyright (c) 2026 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//


#ifndef BOOST_COROSIO_EXAMPLE_JSON_RPC_ERROR_HPP
#define BOOST_COROSIO_EXAMPLE_JSON_RPC_ERROR_HPP

#include <system_error>

namespace json_rpc
{

enum class error
{
  /// Parse error	Invalid JSON was received by the server.
  /// An error occurred on the server while parsing the JSON text.
  parse_error = -32700,
  /// The JSON sent is not a valid Request object.
  invalid_request = -32600, 
  /// The method does not exist / is not available.	
  method_not_found = -32601,
  /// Invalid method parameter(s). 
  invalid_params = -32602,
  /// Internal JSON-RPC error. 
  internal_error = -32603,
  /// Reserved for implementation-defined server-errors. 
  server_error_low  = -32000,
  server_error_high = -32099
};

struct json_rpc_error_category : std::error_category
{
    const char* name() const noexcept override;
    std::string message(int) const override;
    constexpr json_rpc_error_category() noexcept = default;
};

extern json_rpc_error_category json_rpc_error;

}


template<>
struct std::is_error_code_enum<json_rpc::error>
{
    static const bool value = true;
};


#endif

