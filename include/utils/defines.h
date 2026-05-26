#ifndef CORNET_DEFINES_H
#define CORNET_DEFINES_H

// CPU cache line size for alignment purposes
#define CORNET_CACHE_LINE 64

// suppress unused warnings
#define CORNET_MAYBE_UNUSED [[maybe_unused]]
// enforce callers to check return value
#define CORNET_NODISCARD [[nodiscard]]

#endif //CORNET_DEFINES_H
