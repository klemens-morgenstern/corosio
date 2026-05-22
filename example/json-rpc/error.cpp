//
// Copyright (c) 2026 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/cppalliance/corosio
//

#include "error.hpp"

namespace json_rpc
{

const char* json_rpc_error_category::name() const noexcept
{
  return "json-rpc";
}

std::string json_rpc_error_category::message(int v) const
{
  switch (static_cast<error>(v))
  {
    case error::parse_error:
        return "Invalid JSON was received by the server. "
               "An error occurred on the server while parsing the JSON text.";
    case error::invalid_request:
        return "The JSON sent is not a valid Request object.";
    case error::method_not_found:
        return "The method does not exist / is not available.";
    case error::invalid_params:
        return "Invalid method parameter(s).";
    case error::internal_error:
        return "Internal JSON-RPC error.";
    default: 
      return "<unknown json-rpc error>";
  }
}

json_rpc_error_category json_rpc_error;

}

