#include "pipeline/nodes/TcpSinkNode.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <utility>

namespace pipeline {

namespace {

// 将剩余超时裁成单次 poll 的正超时；已到期时返回 0 表示立即返回
int clampPollTimeoutMs(int remaining_ms) {
    if (remaining_ms <= 0) {
        return 0;
    }
    return remaining_ms;
}

} // namespace

TcpSinkNode::TcpSinkNode(const std::string& name, TcpSinkConfig config)
    : SinkNode(name), config_(std::move(config)) {
    addSinkPad("in", TemplateCaps{{MediaType::CONTAINER}});
}

bool TcpSinkNode::onReady() {
    if (config_.host.empty()) {
        postMessage(MessageType::ERROR, "TcpSinkNode: host is empty");
        return false;
    }
    if (config_.port == 0) {
        postMessage(MessageType::ERROR, "TcpSinkNode: port must be non-zero");
        return false;
    }
    if (config_.connect_timeout_ms <= 0) {
        postMessage(MessageType::ERROR, "TcpSinkNode: connect_timeout_ms must be positive");
        return false;
    }
    if (config_.io_poll_timeout_ms <= 0) {
        postMessage(MessageType::ERROR, "TcpSinkNode: io_poll_timeout_ms must be positive");
        return false;
    }

    if (!connectWithTimeout()) {
        // connectWithTimeout 已 postMessage；确保半开 socket 不泄漏给后续 onStop 以外路径
        closeSocket();
        return false;
    }

    bytes_written_ = 0;
    return true;
}

bool TcpSinkNode::onCaps(const std::string&, const CapsEvent& caps,
                         std::vector<QueueItem>*) {
    if (caps.media_type != MediaType::CONTAINER) {
        postMessage(MessageType::ERROR,
                    "TcpSinkNode: input Caps must describe CONTAINER bytes");
        return false;
    }
    return true;
}

void TcpSinkNode::consume(const Buffer* buf) {
    if (sock_ < 0 || !buf || !buf->data || buf->size == 0 ||
        buf->media_type != MediaType::CONTAINER) {
        postMessage(MessageType::ERROR,
                    "TcpSinkNode: received an invalid CONTAINER Buffer");
        return;
    }

    size_t offset = 0;
    while (offset < buf->size && !stop_requested_.load()) {
        // 先等对端接收窗口，再 send；poll 超时只是让出检查 stop 的机会
        if (!waitSocketWritable()) {
            return;
        }
        if (stop_requested_.load()) {
            return;
        }

        const size_t remaining = buf->size - offset;
        const size_t request_size = std::min(
            remaining, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t written = ::send(sock_, buf->data + offset, request_size, MSG_NOSIGNAL);
        if (written > 0) {
            offset += static_cast<size_t>(written);
            bytes_written_ += static_cast<uint64_t>(written);
            continue;
        }
        if (written < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
            continue;
        }

        // 对端关闭或网络错误：可靠推流合同下明确 ERROR，不静默截断
        const int error = written < 0 ? errno : EIO;
        postMessage(MessageType::ERROR,
                    "TcpSinkNode: send failed after " + std::to_string(bytes_written_) +
                        " bytes: " + std::strerror(error), error);
        return;
    }
}

void TcpSinkNode::onDrain() {
    // 自然 EOS：半关闭写端，让 listen 侧 ffmpeg/ffplay 读到 EOF 并完成收尾
    // stop/cancel 路径不会进入 onDrain，避免把强制中断伪装成正常流结束
    if (sock_ < 0) {
        return;
    }
    if (::shutdown(sock_, SHUT_WR) < 0 && errno != ENOTCONN) {
        postMessage(MessageType::ERROR,
                    "TcpSinkNode: shutdown(SHUT_WR) failed: " +
                        std::string(std::strerror(errno)), errno);
    }
}

void TcpSinkNode::onStop() {
    closeSocket();
}

bool TcpSinkNode::connectWithTimeout() {
    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    struct addrinfo* addresses = nullptr;
    const std::string port_text = std::to_string(config_.port);
    const int lookup = ::getaddrinfo(config_.host.c_str(), port_text.c_str(),
                                     &hints, &addresses);
    if (lookup != 0) {
        postMessage(MessageType::ERROR,
                    "TcpSinkNode: getaddrinfo('" + config_.host + "') failed: " +
                        gai_strerror(lookup), lookup);
        return false;
    }

    // 单调时钟测量整段建连，避免多个候选地址把超时放大
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::milliseconds(config_.connect_timeout_ms);
    std::string last_error = "no usable address";

    for (struct addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
        if (std::chrono::steady_clock::now() >= deadline) {
            last_error = "connect timed out";
            break;
        }

        const int candidate = ::socket(address->ai_family,
                                       address->ai_socktype | SOCK_CLOEXEC,
                                       address->ai_protocol);
        if (candidate < 0) {
            last_error = std::strerror(errno);
            continue;
        }

        // 非阻塞 connect：Ready 线程可按剩余超时 poll，不会永久卡在握手
        const int current_flags = ::fcntl(candidate, F_GETFL, 0);
        if (current_flags < 0 ||
            ::fcntl(candidate, F_SETFL, current_flags | O_NONBLOCK) < 0) {
            last_error = std::strerror(errno);
            ::close(candidate);
            continue;
        }

        const int connect_result =
            ::connect(candidate, address->ai_addr, address->ai_addrlen);
        if (connect_result == 0) {
            sock_ = candidate;
            ::freeaddrinfo(addresses);
            return true;
        }
        if (errno != EINPROGRESS) {
            last_error = std::strerror(errno);
            ::close(candidate);
            continue;
        }

        // 同一 candidate 上循环 poll，直到成功、失败或整段建连超时
        bool connected = false;
        bool fatal_timeout = false;
        while (true) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                last_error = "connect timed out";
                fatal_timeout = true;
                break;
            }
            const int slice_ms = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - now).count());

            struct pollfd poll_fd {};
            poll_fd.fd = candidate;
            poll_fd.events = POLLOUT;
            const int poll_result =
                ::poll(&poll_fd, 1, clampPollTimeoutMs(slice_ms));
            if (poll_result == 0) {
                last_error = "connect timed out";
                fatal_timeout = true;
                break;
            }
            if (poll_result < 0) {
                if (errno == EINTR) {
                    // 信号打断后在同一 candidate 上继续，直到 deadline
                    continue;
                }
                last_error = std::strerror(errno);
                break;
            }

            int so_error = 0;
            socklen_t so_len = sizeof(so_error);
            if (::getsockopt(candidate, SOL_SOCKET, SO_ERROR, &so_error, &so_len) < 0) {
                last_error = std::strerror(errno);
                break;
            }
            if (so_error != 0) {
                last_error = std::strerror(so_error);
                break;
            }
            connected = true;
            break;
        }

        if (connected) {
            sock_ = candidate;
            ::freeaddrinfo(addresses);
            return true;
        }

        ::close(candidate);
        if (fatal_timeout) {
            break;
        }
    }

    ::freeaddrinfo(addresses);
    postMessage(MessageType::ERROR,
                "TcpSinkNode: connect " + config_.host + ":" +
                    std::to_string(config_.port) + " failed: " + last_error);
    return false;
}

bool TcpSinkNode::waitSocketWritable() {
    while (!stop_requested_.load()) {
        struct pollfd poll_fd {};
        poll_fd.fd = sock_;
        poll_fd.events = POLLOUT;
        const int poll_result = ::poll(&poll_fd, 1, config_.io_poll_timeout_ms);
        if (poll_result == 0) {
            // 超时：回到循环顶部重新观察 stop_requested_
            continue;
        }
        if (poll_result < 0) {
            if (errno == EINTR) {
                continue;
            }
            postMessage(MessageType::ERROR,
                        "TcpSinkNode: poll failed: " + std::string(std::strerror(errno)),
                        errno);
            return false;
        }
        if (poll_fd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            // 尽量取出 SO_ERROR，便于区分对端重置与本地错误
            int so_error = 0;
            socklen_t so_len = sizeof(so_error);
            if (::getsockopt(sock_, SOL_SOCKET, SO_ERROR, &so_error, &so_len) == 0 &&
                so_error != 0) {
                postMessage(MessageType::ERROR,
                            "TcpSinkNode: socket error while waiting to send: " +
                                std::string(std::strerror(so_error)), so_error);
            } else {
                postMessage(MessageType::ERROR,
                            "TcpSinkNode: socket closed while waiting to send");
            }
            return false;
        }
        if (poll_fd.revents & POLLOUT) {
            return true;
        }
    }
    return false;
}

void TcpSinkNode::closeSocket() {
    if (sock_ < 0) {
        return;
    }

    // Linux close 被 EINTR 打断后不能安全重试，避免误关复用后的 fd
    ::close(sock_);
    sock_ = -1;
}

} // namespace pipeline
