#pragma once

#include <sys/mman.h>

class MappedMemory {
    public:
    void*  addr;
    size_t length;
    size_t refs;
    MappedMemory(void* addr, size_t length)
    : addr(addr)
    , length(length)
    , refs(0) {}

    MappedMemory(const MappedMemory&) = delete;
    MappedMemory& operator=(const MappedMemory&) = delete;

    MappedMemory(MappedMemory&& other) noexcept
    : addr(other.addr)
    , length(other.length)
    , refs(other.refs) {
        other.addr = nullptr;
        other.length = 0;
        other.refs = 0;
    }

    MappedMemory& operator=(MappedMemory&& other) noexcept {
        if (this != &other) {
            if (addr != nullptr) {
                munmap(addr, length);
            }
            addr = other.addr;
            length = other.length;
            refs = other.refs;
            other.addr = nullptr;
            other.length = 0;
            other.refs = 0;
        }
        return *this;
    }

    ~MappedMemory() {
        munmap(addr, length);
    }
};