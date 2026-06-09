#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace pipeline {

// ===== 命令类型 ID =====
namespace CmdId {
    constexpr uint32_t STOP = 0;
    // 未来内置命令在此扩展
    // constexpr uint32_t PAUSE = 1;
    // constexpr uint32_t SEEK  = 2;

    // 用户自定义命令从 1000 开始
    constexpr uint32_t USER_BASE = 1000;
}

// ===== 命令基类 =====
class Command {
public:
    virtual ~Command() = default;
    virtual uint32_t typeId() const = 0;

protected:
    Command() = default;
};

// ===== 内置命令：停止 =====
class StopCommand : public Command {
public:
    uint32_t typeId() const override { return CmdId::STOP; }
};

} // namespace pipeline
