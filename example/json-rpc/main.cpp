#if 0

#include <boost/corosio/tcp_server.hpp>
#include <boost/capy/task.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/write.hpp>

#include "protocol.hpp"

namespace corosio = boost::corosio;
namespace capy = boost::capy;

double add(double i, double j) noexcept { return i + j; }
double sub(double i, double j) noexcept { return i / j; }
double mul(double i, double j) noexcept { return i * j; }
double div(double i, double j) noexcept { return i / j; }



/*

int main(int argc, char* argv[])
{
  corosio::io_context ioc;

  std::unordered_map<std::string, std::unique_ptr<json_rpc::function_t>> funcs;

  funcs.emplace("add", json_rpc::make_function(&add, "i", "j"));
  funcs.emplace("sub", json_rpc::make_function(&sub, "i", "j"));
  funcs.emplace("mul", json_rpc::make_function(&mul, "i", "j"));
  funcs.emplace("div", json_rpc::make_function(&div, "i", "j"));


  ioc.run();
  return 0;
}*/

#else
#include <boost/corosio.hpp>
#include <boost/capy/task.hpp>
#include <boost/capy/ex/run_async.hpp>
#include <boost/capy/buffers.hpp>
#include <boost/capy/buffers/make_buffer.hpp>
#include <boost/capy/buffers/string_dynamic_buffer.hpp>
#include <boost/capy/error.hpp>
#include <boost/capy/read.hpp>
#include <boost/capy/write.hpp>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>

namespace corosio = boost::corosio;
namespace capy = boost::capy;

capy::task<void>
do_request(corosio::io_stream& stream, std::string_view host)
{
    std::string request =
        "GET / HTTP/1.1\r\n"
        "Host: " + std::string(host) + "\r\n"
        "Connection: close\r\n"
        "\r\n";

    if (auto [ec_write, n_write] = co_await capy::write(
            stream, capy::make_buffer(request)); ec_write)
        throw std::system_error(ec_write);

    std::string response;
    auto [ec_read, n_read] = co_await capy::read(
        stream, capy::string_dynamic_buffer(&response));
    if (ec_read && ec_read != capy::error::eof)
        throw std::system_error(ec_read);
}

capy::task<void>
run_client(
    corosio::io_context& ioc,
    std::string_view host,
    std::string_view service)
{
    corosio::resolver resolver(ioc);
    auto [ec_resolve, results] = co_await resolver.resolve(host, service);
    if (ec_resolve)
        throw std::system_error(ec_resolve);

    auto endpoint = results.begin()->get_endpoint();

    corosio::tcp_socket socket(ioc);
    socket.open(endpoint.is_v6() ? corosio::tcp::v6() : corosio::tcp::v4());

    auto [ec_connect] = co_await socket.connect(endpoint);

    if (ec_connect)
        throw std::system_error(ec_connect);

    co_await do_request(socket, host);
}

int main()
{
    corosio::io_context ioc;
    capy::run_async(ioc.get_executor())(
        run_client(ioc, "example.org", "http"));
    ioc.run();
    return 0;
}

#endif 
