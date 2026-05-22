#
# Copyright (c) 2026 Vinnie Falco (vinnie.falco@gmail.com)
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
#
# Official repository: https://github.com/cppalliance/corosio
#

add_library(corosio_beast beast.cpp)
target_link_libraries(corosio_beast PUBLIC Boost::corosio Boost::corosio_openssl Boost::asio Boost::beast)

add_executable(corosio_example_beast_client http_client.cpp)
target_link_libraries(corosio_example_beast_client
    corosio_beast Boost::corosio Boost::url)
set_property(TARGET corosio_example_beast_client PROPERTY FOLDER "examples")

add_executable(corosio_example_beast_server_sync http_server_sync.cpp)
target_link_libraries(corosio_example_beast_server_sync
    corosio_beast_core Boost::corosio Boost::url)
set_property(TARGET corosio_example_beast_server_sync
    PROPERTY FOLDER "examples")


