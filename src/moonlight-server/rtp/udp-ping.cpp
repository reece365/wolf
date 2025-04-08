#include <boost/bind/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <helpers/logger.hpp>
#include <rtp/udp-ping.hpp>
#include <thread>

namespace rtp {

void UDP_Server::start_receive() {
  socket_.async_receive_from(boost::asio::buffer(recv_buffer_),
                             remote_endpoint_,
                             0,
                             boost::bind(&UDP_Server::handle_receive,
                                         this,
                                         boost::asio::placeholders::error,
                                         boost::asio::placeholders::bytes_transferred));
}

void UDP_Server::handle_receive(const boost::system::error_code &error, std::size_t bytes_transferred) {
  if (!error) {
    auto client_ip = remote_endpoint_.address().to_string();
    auto client_port = remote_endpoint_.port();

    logs::log(logs::trace, "[RTP] Received ping from {}:{} ({} bytes)", client_ip, client_port, bytes_transferred);

    if (bytes_transferred == 4) {
      callback({.client_ip = client_ip, .client_port = client_port, .payload = {}});
    } else if (bytes_transferred >= sizeof(moonlight::SS_PING)) {
      auto ping = (moonlight::SS_PING *)recv_buffer_.data();
      callback({.client_ip = client_ip, .client_port = client_port, .payload = ping->payload});
    }
    // Continue to receive more data
    start_receive();
  } else {
    logs::log(logs::warning, "[RTP] Error receiving ping: {}", error.message());
  }
}

void start_rtp_ping(unsigned short video_port,
                    unsigned short audio_port,
                    std::shared_ptr<wolf::core::events::EventBusType> event_bus) {
  try {
    std::shared_ptr<boost::asio::io_context> io_context = std::make_shared<boost::asio::io_context>();
    std::shared_ptr<udp::socket> video_socket =
        std::make_shared<udp::socket>(*io_context, udp::endpoint(udp::v4(), video_port));
    video_socket->set_option(udp::socket::reuse_address(true));

    std::shared_ptr<udp::socket> audio_socket =
        std::make_shared<udp::socket>(*io_context, udp::endpoint(udp::v4(), audio_port));
    audio_socket->set_option(udp::socket::reuse_address(true));

    std::thread([io_context, video_socket, audio_socket, event_bus]() {
      UDP_Server video_server(*video_socket, [event_bus](const RTPPingEvent &ping) {
        logs::log(logs::trace, "[RTP] video from {}:{}", ping.client_ip, ping.client_port);
        auto ev = wolf::core::events::RTPVideoPingEvent{.client_ip = ping.client_ip,
                                                        .client_port = ping.client_port,
                                                        .payload = ping.payload};
        event_bus->fire_event(immer::box<wolf::core::events::RTPVideoPingEvent>(ev));
      });

      UDP_Server audio_server(*audio_socket, [event_bus](const RTPPingEvent &ping) {
        logs::log(logs::trace, "[RTP] audio  from {}:{}", ping.client_ip, ping.client_port);
        auto ev = wolf::core::events::RTPAudioPingEvent{.client_ip = ping.client_ip,
                                                        .client_port = ping.client_port,
                                                        .payload = ping.payload};
        event_bus->fire_event(immer::box<wolf::core::events::RTPAudioPingEvent>(ev));
      });

      io_context->run();
      logs::log(logs::info, "[RTP] io_context stopped");
    }).detach();
  } catch (std::exception &e) {
    logs::log(logs::warning, "[RTP] Unable to start RTP server: {}", e.what());
  }
}

} // namespace rtp