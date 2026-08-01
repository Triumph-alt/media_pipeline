#include "pipeline/nodes/FileSinkNode.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

namespace pipeline {

FileSinkNode::FileSinkNode(const std::string& name, FileSinkConfig config)
    : SinkNode(name), config_(std::move(config)) {
    addSinkPad("in", TemplateCaps{{MediaType::CONTAINER}});
}

bool FileSinkNode::onReady() {
    if (config_.path.empty()) {
        postMessage(MessageType::ERROR, "FileSinkNode: output path is empty");
        return false;
    }

    if (config_.overwrite) {
        struct stat existing{};
        const int lstat_result = lstat(config_.path.c_str(), &existing);
        if (lstat_result == 0 && !S_ISREG(existing.st_mode)) {
            postMessage(MessageType::ERROR,
                        "FileSinkNode: overwrite target is not a regular file");
            return false;
        }
        if (lstat_result < 0 && errno != ENOENT) {
            postMessage(MessageType::ERROR,
                        "FileSinkNode: lstat failed: " + std::string(std::strerror(errno)));
            return false;
        }
    }

    const int flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_NOFOLLOW |
        (config_.overwrite ? O_TRUNC : O_EXCL);
    fd_ = open(config_.path.c_str(), flags, 0644);
    if (fd_ < 0) {
        postMessage(MessageType::ERROR,
                    "FileSinkNode: open '" + config_.path + "' failed: " +
                        std::strerror(errno));
        return false;
    }

    struct stat status{};
    if (fstat(fd_, &status) < 0) {
        postMessage(MessageType::ERROR,
                    "FileSinkNode: fstat failed: " + std::string(std::strerror(errno)));
        return false;
    }
    if (!S_ISREG(status.st_mode)) {
        postMessage(MessageType::ERROR,
                    "FileSinkNode: output path is not a regular file");
        return false;
    }

    bytes_written_ = 0;
    return true;
}

bool FileSinkNode::onCaps(const std::string&, const CapsEvent& caps,
                          std::vector<QueueItem>*) {
    if (caps.media_type != MediaType::CONTAINER) {
        postMessage(MessageType::ERROR,
                    "FileSinkNode: input Caps must describe CONTAINER bytes");
        return false;
    }
    return true;
}

void FileSinkNode::consume(const Buffer* buf) {
    if (fd_ < 0 || !buf || !buf->data || buf->size == 0 ||
        buf->media_type != MediaType::CONTAINER) {
        postMessage(MessageType::ERROR,
                    "FileSinkNode: received an invalid CONTAINER Buffer");
        return;
    }

    size_t offset = 0;
    while (offset < buf->size && !stop_requested_.load()) {
        const size_t remaining = buf->size - offset;
        const size_t request_size = std::min(
            remaining, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        const ssize_t written = write(fd_, buf->data + offset, request_size);
        if (written > 0) {
            offset += static_cast<size_t>(written);
            bytes_written_ += static_cast<uint64_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }

        const int error = written < 0 ? errno : EIO;
        postMessage(MessageType::ERROR,
                    "FileSinkNode: write failed after " + std::to_string(bytes_written_) +
                        " bytes: " + std::strerror(error), error);
        return;
    }
}

bool FileSinkNode::syncFile() {
    if (fd_ < 0) {
        postMessage(MessageType::ERROR,
                    "FileSinkNode: output file is unavailable while syncing");
        return false;
    }

    while (fsync(fd_) < 0) {
        if (errno == EINTR) {
            continue;
        }
        postMessage(MessageType::ERROR,
                    "FileSinkNode: fsync failed: " + std::string(std::strerror(errno)), errno);
        return false;
    }
    return true;
}

void FileSinkNode::onDrain() {
    // 自然 EOS 才进入 onDrain，先确保 Mux Header/Packet/Trailer 字节持久化再上报 Sink EOS
    syncFile();
}

void FileSinkNode::onStop() {
    closeFile();
}

void FileSinkNode::closeFile() {
    if (fd_ < 0) {
        return;
    }

    // Linux close() 即使被 EINTR 打断也不能安全重试，同一个数字可能已被其他线程复用
    close(fd_);
    fd_ = -1;
}

} // namespace pipeline
