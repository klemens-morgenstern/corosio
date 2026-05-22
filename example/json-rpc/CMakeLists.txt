#
# Copyright (c) 2026 Vinnie Falco (vinnie dot falco at gmail dot com)
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
#
# Official repository: https://github.com/cppalliance/corosio
#

add_executable(corosio_example_json_rpc
    json-io.cpp error.cpp main.cpp)

target_include_directories(corosio_example_json_rpc PUBLIC "/home/klemens/develop/boost")

set_property(TARGET corosio_example_json_rpc
    PROPERTY FOLDER "examples")

target_link_libraries(corosio_example_json_rpc
    Boost::corosio)

